package com.github.highcumontoa.taskqueueruntimejava.tests;

import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.error.QueueException;
import com.github.highcumontoa.taskqueueruntimejava.model.BackpressureStrategy;
import com.github.highcumontoa.taskqueueruntimejava.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.model.DeliveryReason;
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
 * 长跑回收：终结消息按保留期安全回收，占用有界、位点单调不倒退；
 * 落出保留范围的重放被明确拒绝（OFFSET_COMPACTED），不悄悄跳过。
 */
class RetentionReclaimTest {

    private static final Logger log = LoggerFactory.getLogger(RetentionReclaimTest.class);
    private static final String T = "longrun";
    private static final String G = "g";

    private RuntimeTestSupport.Env env(int capacity, Duration retention) {
        var env = RuntimeTestSupport.memoryRuntime();
        env.runtime().createTopic(ADMIN, T, new TopicConfig(capacity,
                BackpressureStrategy.REJECT, Duration.ofSeconds(1), Duration.ofSeconds(30),
                com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1,
                retention, 200));
        env.runtime().createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env, T);
        return env;
    }

    @Test
    void longRun_occupationBoundedAndOffsetsMonotonic() {
        int capacity = 20;
        var env = env(capacity, Duration.ofSeconds(5));
        TaskQueueRuntime rt = env.runtime();
        int rounds = 300;
        long maxRetained = 0;
        for (int i = 0; i < rounds; i++) {
            var receipt = rt.produce(PRODUCER, T, "payload-" + i, null);
            assertEquals(i, receipt.getOffset(), "位点必须单调递增不倒退");
            Delivery d = rt.poll(CONSUMER, T, G);
            assertNotNull(d);
            rt.commit(CONSUMER, T, G, d.getDeliveryId());
            // 模拟时间流逝并周期性回收（真实部署由定时任务驱动 reclaimTerminated）。
            env.clock().advance(Duration.ofSeconds(1));
            if (i % 5 == 4) {
                rt.reclaimTerminated(T);
            }
            long retained = env.backend().readTopic(T).getMessages().size();
            maxRetained = Math.max(maxRetained, retained);
            assertTrue(retained <= capacity,
                    "保留消息数不得越过容量上限 retained=" + retained);
        }
        var state = env.backend().readTopic(T);
        log.info("long-run finished rounds={} maxRetained={} finalRetained={} baseOffset={} nextOffset={} committed={}",
                rounds, maxRetained, state.getMessages().size(), state.getBaseOffset(),
                state.getNextOffset(), rt.offsetOf(CONSUMER, T, G).getCommitted());
        assertEquals(rounds, state.getNextOffset(), "生产位点持续推进");
        assertTrue(state.getBaseOffset() > rounds - capacity,
                "大部分终结消息应已被回收 baseOffset=" + state.getBaseOffset());
        assertEquals(rounds - 1, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "消费水位单调推进到末尾，不倒退");
        assertTrue(state.getMessages().size() <= capacity, "长跑后占用有界");
    }

    @Test
    void replayIntoCompactedRange_rejectedDistinctly_butRetainedRangeReplayable() {
        var env = env(100, Duration.ofSeconds(10));
        TaskQueueRuntime rt = env.runtime();
        // 第一批 6 条全部消费确认，随后时间推进超过保留期并被回收。
        for (int i = 0; i < 6; i++) {
            rt.produce(PRODUCER, T, "old-" + i, null);
        }
        for (int i = 0; i < 6; i++) {
            Delivery d = rt.poll(CONSUMER, T, G);
            rt.commit(CONSUMER, T, G, d.getDeliveryId());
        }
        env.clock().advance(Duration.ofSeconds(30));
        int reclaimed = rt.reclaimTerminated(T);
        assertEquals(6, reclaimed, "6 条终结消息应被回收");
        assertEquals(6, env.backend().readTopic(T).getBaseOffset());

        // 第二批 3 条（offset 6..8）确认后仍在保留期内。
        for (int i = 0; i < 3; i++) {
            rt.produce(PRODUCER, T, "new-" + i, null);
        }
        for (int i = 0; i < 3; i++) {
            Delivery d = rt.poll(CONSUMER, T, G);
            rt.commit(CONSUMER, T, G, d.getDeliveryId());
        }
        assertEquals(8, rt.offsetOf(CONSUMER, T, G).getCommitted());

        // 重放到已回收区间：明确拒绝，错误码可区分，绝不悄悄跳过。
        QueueException compacted = assertThrows(QueueException.class,
                () -> rt.replay(ADMIN, T, G, -1));
        assertEquals(ErrorCode.OFFSET_COMPACTED, compacted.getCode(),
                "落出保留范围的重放必须以 OFFSET_COMPACTED 明确拒绝");
        log.warn("replay into compacted range rejected code={} reason={}",
                compacted.getCode(), compacted.getMessage());

        // 保留范围内的重放（target = baseOffset-1 = 5）仍可用，覆盖 6..8。
        var info = rt.replay(ADMIN, T, G, 5);
        assertEquals(5, info.getCommitted());
        Delivery replayed = rt.poll(CONSUMER, T, G);
        assertNotNull(replayed);
        assertEquals(6, replayed.getOffset());
        assertEquals(DeliveryReason.REPLAY, replayed.getReason());
        log.info("replay within retention ok group={} firstOffset={} reason={}",
                G, replayed.getOffset(), replayed.getReason());
    }

    @Test
    void deadLetters_reclaimedAfterRetention() {
        var env = env(100, Duration.ofSeconds(10));
        TaskQueueRuntime rt = env.runtime();
        rt.produce(PRODUCER, T, "poison", null);
        Delivery d = rt.poll(CONSUMER, T, G);
        var nr = rt.nack(CONSUMER, T, G, d.getDeliveryId(), "bad payload", false);
        assertEquals(TaskQueueRuntime.NackResult.Status.DEAD_LETTER, nr.getStatus());
        assertEquals(1, rt.deadLetters(CONSUMER, T).size(), "死信应先可查询");

        // 保留期内不回收；超过保留期后消息与死信记录都被安全清理。
        rt.reclaimTerminated(T);
        assertEquals(1, rt.deadLetters(CONSUMER, T).size(), "保留期内不得回收死信");
        env.clock().advance(Duration.ofSeconds(30));
        int reclaimed = rt.reclaimTerminated(T);
        assertEquals(1, reclaimed, "死信消息本体应被回收");
        assertEquals(0, rt.deadLetters(CONSUMER, T).size(), "超期死信记录应被清理");
        assertEquals(1, env.backend().readTopic(T).getBaseOffset());
        log.info("dead letter reclaimed topic={} baseOffset={} remainingDeadLetters={}",
                T, env.backend().readTopic(T).getBaseOffset(), rt.deadLetters(CONSUMER, T).size());
    }

    @Test
    void capacity_hardBoundsRetained_underPressureReclaim() {
        // 保留期很长时，容量承压会提前回收终结消息，容量始终硬约束占用。
        var env = env(5, Duration.ofHours(1));
        TaskQueueRuntime rt = env.runtime();
        for (int i = 0; i < 5; i++) {
            rt.produce(PRODUCER, T, "m" + i, null);
        }
        QueueException full = assertThrows(QueueException.class,
                () -> rt.produce(PRODUCER, T, "overflow", null));
        assertEquals(ErrorCode.QUEUE_FULL, full.getCode(),
                "保留期内的终结消息也计入占用，容量是硬上限");

        // 确认全部 5 条：承压回收立即生效（不等保留期），生产恢复。
        for (int i = 0; i < 5; i++) {
            Delivery d = rt.poll(CONSUMER, T, G);
            rt.commit(CONSUMER, T, G, d.getDeliveryId());
        }
        var receipt = rt.produce(PRODUCER, T, "after-reclaim", null);
        assertEquals(5, receipt.getOffset(), "位点单调不倒退");
        assertTrue(env.backend().readTopic(T).getMessages().size() <= 5,
                "承压回收后占用仍受容量约束");
        log.info("pressure reclaim freed capacity topic={} baseOffset={} nextOffset={}",
                T, env.backend().readTopic(T).getBaseOffset(), receipt.getOffset());
    }
}
