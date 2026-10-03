package com.github.highcumontoa.taskqueueruntimejava.tests;

import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.error.QueueException;
import com.github.highcumontoa.taskqueueruntimejava.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.model.TopicConfig;
import com.github.highcumontoa.taskqueueruntimejava.runtime.TaskQueueRuntime;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.ADMIN;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.CONSUMER;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.PRODUCER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 长跑占用有界与保留边界：持续生产/消费/提交时，已终结消息被安全回收，
 * 容量上限真实约束长期占用；位点单调不倒退；对已被清出保留范围的历史
 * 重放必须以可区分的错误明确拒绝。
 */
class RetentionTest {

    private static final Logger log = LoggerFactory.getLogger(RetentionTest.class);
    private static final String T = "longrun";
    private static final String G = "g";

    private RuntimeTestSupport.Env env(int capacity) {
        var env = RuntimeTestSupport.memoryRuntime();
        env.runtime().createTopic(ADMIN, T, new TopicConfig(capacity, null, Duration.ofSeconds(1),
                Duration.ofMinutes(10),
                com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1));
        env.runtime().createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env, T);
        return env;
    }

    @Test
    void longRun_highThroughput_footprintStaysBounded() {
        int capacity = 50;
        var env = env(capacity);
        TaskQueueRuntime rt = env.runtime();
        int total = 2000;
        int maxObserved = 0;
        for (int i = 0; i < total; i++) {
            rt.produce(PRODUCER, T, "m" + i, null);
            Delivery d = rt.poll(CONSUMER, T, G);
            assertNotNull(d);
            rt.commit(CONSUMER, T, G, d.getDeliveryId());
            int retained = env.backend().readTopic(T).getMessages().size();
            maxObserved = Math.max(maxObserved, retained);
            assertTrue(retained <= capacity,
                    "保留占用不得越过容量上限: retained=" + retained + " capacity=" + capacity);
        }
        var info = rt.offsetOf(CONSUMER, T, G);
        assertEquals(total - 1, info.getCommitted(), "长跑后位点必须持续单调推进");
        assertEquals(total, env.backend().readTopic(T).getNextOffset(),
                "位点/offset 序列不得因回收而倒退或重用");
        assertTrue(info.getEarliestRetained() > 0, "已终结前缀必须被回收，保留边界前移");
        assertTrue(env.backend().readTopic(T).getMessages().size() <= capacity);
        log.info("long run done total={} maxRetained={} capacity={} committed={} retainedFrom={}",
                total, maxObserved, capacity, info.getCommitted(), info.getEarliestRetained());
    }

    @Test
    void replay_belowRetentionBoundary_isRejectedDistinctly() {
        var env = env(10);
        TaskQueueRuntime rt = env.runtime();
        // 生产并提交 5 条（offset 0..4）。
        for (int i = 0; i < 5; i++) {
            rt.produce(PRODUCER, T, "early-" + i, null);
            Delivery d = rt.poll(CONSUMER, T, G);
            rt.commit(CONSUMER, T, G, d.getDeliveryId());
        }
        // 继续生产触发回收：0..4 终结前缀被清出保留范围。
        for (int i = 0; i < 10; i++) {
            rt.produce(PRODUCER, T, "later-" + i, null);
        }
        long retainedFrom = rt.offsetOf(CONSUMER, T, G).getEarliestRetained();
        assertEquals(5, retainedFrom, "终结前缀应已被回收");
        log.info("reclaim happened retainedFrom={}", retainedFrom);

        // 消费并提交到 offset 9，使 committed=9 > retainedFrom-1。
        for (int i = 0; i < 5; i++) {
            Delivery d = rt.poll(CONSUMER, T, G);
            rt.commit(CONSUMER, T, G, d.getDeliveryId());
        }
        assertEquals(9, rt.offsetOf(CONSUMER, T, G).getCommitted());

        // 对已回收历史的重放：明确拒绝，错误码与“无变化回退”可区分。
        QueueException outOfRetention = assertThrows(QueueException.class,
                () -> rt.replay(ADMIN, T, G, 3));
        assertEquals(ErrorCode.OFFSET_OUT_OF_RETENTION, outOfRetention.getCode(),
                "对已清出保留范围的历史重放必须以独立错误码拒绝");
        QueueException fromHead = assertThrows(QueueException.class,
                () -> rt.replay(ADMIN, T, G, -1));
        assertEquals(ErrorCode.OFFSET_OUT_OF_RETENTION, fromHead.getCode());
        QueueException noOp = assertThrows(QueueException.class,
                () -> rt.replay(ADMIN, T, G, 9));
        assertEquals(ErrorCode.OFFSET_ROLLBACK_REJECTED, noOp.getCode(),
                "无变化回退仍是另一种可区分结论");
        log.warn("replay rejected distinctly outOfRetention={} noOp={}",
                outOfRetention.getCode(), noOp.getCode());

        // 保留范围内的重放不受影响：target=4 覆盖 (4, 9]，全部仍在保留区间。
        var info = rt.replay(ADMIN, T, G, 4);
        assertEquals(4, info.getCommitted());
        Delivery replayed = rt.poll(CONSUMER, T, G);
        assertNotNull(replayed);
        assertEquals(5, replayed.getOffset(), "保留范围内重放必须从 offset 5 开始");
        log.info("in-retention replay ok committed={} firstRedeliveryOffset={}",
                info.getCommitted(), replayed.getOffset());
    }

    @Test
    void deadLetters_areReclaimedWithTheirMessages() {
        var env = env(10);
        TaskQueueRuntime rt = env.runtime();
        rt.produce(PRODUCER, T, "poison", null);
        Delivery d = rt.poll(CONSUMER, T, G);
        var nack = rt.nack(CONSUMER, T, G, d.getDeliveryId(), "poison", false);
        assertEquals(TaskQueueRuntime.NackResult.Status.DEAD_LETTER, nack.getStatus());
        assertEquals(1, rt.deadLetters(CONSUMER, T).size(), "死信在保留窗口内必须可查询");

        // 后续生产触发回收：死信消息终结，连同死信记录一起被清出保留范围。
        rt.produce(PRODUCER, T, "next", null);
        assertEquals(0, rt.deadLetters(CONSUMER, T).size(),
                "死信记录随消息回收一并清理，不无限累积");
        assertEquals(1, rt.offsetOf(CONSUMER, T, G).getEarliestRetained());
        log.info("dead letter reclaimed with message retainedFrom={}",
                rt.offsetOf(CONSUMER, T, G).getEarliestRetained());
    }

    @Test
    void explicitReclaim_andNewGroupSeesOnlyRetained() {
        var env = env(100);
        TaskQueueRuntime rt = env.runtime();
        for (int i = 0; i < 5; i++) {
            rt.produce(PRODUCER, T, "m" + i, null);
            Delivery d = rt.poll(CONSUMER, T, G);
            rt.commit(CONSUMER, T, G, d.getDeliveryId());
        }
        // 生产路径已自动回收大部分终结前缀；显式驱动清掉剩余部分。
        int reclaimed = rt.reclaimFinished(T);
        assertTrue(reclaimed >= 0);
        assertEquals(5, rt.offsetOf(CONSUMER, T, G).getEarliestRetained(),
                "自动+显式回收后，保留边界必须越过全部终结前缀");
        assertEquals(4, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "回收不得导致位点倒退");

        // 回收后新建的消费者组只能看到保留范围内的消息。
        rt.createGroup(ADMIN, T, "late-joiner");
        assertEquals(null, rt.poll(CONSUMER, T, "late-joiner"),
                "已回收的历史对新组不可见");
        rt.produce(PRODUCER, T, "fresh", null);
        Delivery fresh = rt.poll(CONSUMER, T, "late-joiner");
        assertNotNull(fresh);
        assertEquals(5, fresh.getOffset(), "新组从保留边界之后开始消费");
        log.info("late joiner starts at retained boundary offset={}", fresh.getOffset());
    }
}
