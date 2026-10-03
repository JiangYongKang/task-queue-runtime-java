package com.github.highcumontoa.taskqueueruntimejava.tests;

import com.github.highcumontoa.taskqueueruntimejava.model.CommitOutcome;
import com.github.highcumontoa.taskqueueruntimejava.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.model.DeliveryReason;
import com.github.highcumontoa.taskqueueruntimejava.model.TopicConfig;
import com.github.highcumontoa.taskqueueruntimejava.runtime.TaskQueueRuntime;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.ADMIN;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.CONSUMER;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.PRODUCER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 乱序提交下的位点安全：并发消费者处理快慢不一，确认到达顺序与消息顺序不一致时，
 * 对外可见位点（连续水位线）不得跳过仍在处理中的更早消息；
 * 重启恢复后从最早未完成的消息继续，已确认的不重复、中间的不跳过。
 */
class OutOfOrderCommitTest {

    private static final Logger log = LoggerFactory.getLogger(OutOfOrderCommitTest.class);
    private static final String T = "parallel-events";
    private static final String G = "workers";

    private RuntimeTestSupport.Env env() {
        var env = RuntimeTestSupport.memoryRuntime();
        env.runtime().createTopic(ADMIN, T, new TopicConfig(100, null, Duration.ofSeconds(1),
                Duration.ofMinutes(10),
                com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1));
        env.runtime().createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env, T);
        return env;
    }

    @Test
    void outOfOrderCommits_watermarkDoesNotSkipInflightGap() {
        var env = env();
        TaskQueueRuntime rt = env.runtime();
        for (int i = 0; i < 3; i++) {
            rt.produce(PRODUCER, T, "m" + i, null);
        }
        // 模拟并发消费：三条消息都被拉走，处理快慢不同。
        Delivery d0 = rt.poll(CONSUMER, T, G);
        Delivery d1 = rt.poll(CONSUMER, T, G);
        Delivery d2 = rt.poll(CONSUMER, T, G);
        assertEquals(0, d0.getOffset());
        assertEquals(1, d1.getOffset());
        assertEquals(2, d2.getOffset());

        // 后到的 offset=2 先确认：位点不得越过仍在处理中的 0 和 1。
        var c2 = rt.commit(CONSUMER, T, G, d2.getDeliveryId());
        assertEquals(CommitOutcome.COMMITTED, c2.getOutcome());
        assertEquals(-1, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "乱序提交不得跳过仍在处理中的更早消息");
        log.info("out-of-order commit offset=2 committed={} (gap at 0,1)",
                rt.offsetOf(CONSUMER, T, G).getCommitted());

        // 缺口被部分填补：位点推进到连续已终结处为止。
        rt.commit(CONSUMER, T, G, d0.getDeliveryId());
        assertEquals(0, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "offset=1 仍在处理，位点只能推进到 0");

        rt.commit(CONSUMER, T, G, d1.getDeliveryId());
        assertEquals(2, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "缺口补齐后位点一次性越过已确认的 offset=2");
        log.info("gap filled, watermark advanced committed={}",
                rt.offsetOf(CONSUMER, T, G).getCommitted());
    }

    @Test
    void restart_afterOutOfOrderCommits_resumesFromEarliestIncomplete() throws Exception {
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("queue-ooo-");
        var backend = new com.github.highcumontoa.taskqueueruntimejava.backend.LocalFileQueueBackend(dir);
        var env1 = RuntimeTestSupport.newRuntime(backend);
        env1.runtime().createTopic(ADMIN, T, new TopicConfig(100, null, Duration.ofSeconds(1),
                Duration.ofMinutes(10),
                com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1));
        env1.runtime().createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env1, T);
        for (int i = 0; i < 4; i++) {
            env1.runtime().produce(PRODUCER, T, "m" + i, null);
        }
        Delivery d0 = env1.runtime().poll(CONSUMER, T, G);
        Delivery d1 = env1.runtime().poll(CONSUMER, T, G);
        Delivery d2 = env1.runtime().poll(CONSUMER, T, G);
        // 乱序确认：0 和 2 已确认，1 仍在处理中。
        env1.runtime().commit(CONSUMER, T, G, d0.getDeliveryId());
        env1.runtime().commit(CONSUMER, T, G, d2.getDeliveryId());
        assertEquals(0, env1.runtime().offsetOf(CONSUMER, T, G).getCommitted());
        // 进程退出：在途的 offset=1 被安全交还。
        env1.runtime().close(Duration.ZERO);
        log.info("shutdown with gap: committed=0 inflight offset=1 messageId={}", d1.getMessageId());

        // 重启恢复：位点保持 0，从最早未完成的 offset=1 继续投递。
        var backend2 = new com.github.highcumontoa.taskqueueruntimejava.backend.LocalFileQueueBackend(dir);
        backend2.recover();
        var env2 = RuntimeTestSupport.newRuntime(backend2);
        RuntimeTestSupport.grantTopic(env2, T);
        TaskQueueRuntime rt2 = env2.runtime();
        assertEquals(0, rt2.offsetOf(CONSUMER, T, G).getCommitted(),
                "重启后位点不得跳变也不得倒退");

        Delivery redelivered = rt2.poll(CONSUMER, T, G);
        assertNotNull(redelivered);
        assertEquals(1, redelivered.getOffset(), "必须从最早未完成的消息继续，不得跳过");
        assertEquals(DeliveryReason.VISIBILITY_TIMEOUT, redelivered.getReason());
        log.info("resumed messageId={} offset={} reason={}",
                redelivered.getMessageId(), redelivered.getOffset(), redelivered.getReason());
        rt2.commit(CONSUMER, T, G, redelivered.getDeliveryId());
        assertEquals(2, rt2.offsetOf(CONSUMER, T, G).getCommitted(),
                "缺口补齐后位点越过此前已确认的 offset=2");

        // 已确认的 0、2 不得重复投递；下一条只能是 offset=3。
        Delivery next = rt2.poll(CONSUMER, T, G);
        assertNotNull(next);
        assertEquals(3, next.getOffset(), "已确认消息不得重复投递，中间不得跳过");
        rt2.commit(CONSUMER, T, G, next.getDeliveryId());
        assertEquals(3, rt2.offsetOf(CONSUMER, T, G).getCommitted());
        assertNull(rt2.poll(CONSUMER, T, G), "全部完成后不应再有投递");
        log.info("restart recovery done committed={}", rt2.offsetOf(CONSUMER, T, G).getCommitted());
        rt2.close(Duration.ZERO);
    }

    @Test
    void replay_stillCoversGapRange_afterOutOfOrderCommits() {
        var env = env();
        TaskQueueRuntime rt = env.runtime();
        for (int i = 0; i < 3; i++) {
            rt.produce(PRODUCER, T, "m" + i, null);
        }
        List<Delivery> deliveries = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            deliveries.add(rt.poll(CONSUMER, T, G));
        }
        // 乱序确认全部完成：2, 0, 1。
        rt.commit(CONSUMER, T, G, deliveries.get(2).getDeliveryId());
        rt.commit(CONSUMER, T, G, deliveries.get(0).getDeliveryId());
        rt.commit(CONSUMER, T, G, deliveries.get(1).getDeliveryId());
        assertEquals(2, rt.offsetOf(CONSUMER, T, G).getCommitted());

        // 显式重放仍能覆盖这段区间：从头重放，按 offset 顺序重新投递。
        rt.replay(ADMIN, T, G, -1);
        for (int i = 0; i < 3; i++) {
            Delivery replayed = rt.poll(CONSUMER, T, G);
            assertNotNull(replayed);
            assertEquals(i, replayed.getOffset());
            assertEquals(DeliveryReason.REPLAY, replayed.getReason());
            rt.commit(CONSUMER, T, G, replayed.getDeliveryId());
        }
        assertEquals(2, rt.offsetOf(CONSUMER, T, G).getCommitted());
        log.info("replay covered gap range, redelivered 3 messages in order");
    }
}
