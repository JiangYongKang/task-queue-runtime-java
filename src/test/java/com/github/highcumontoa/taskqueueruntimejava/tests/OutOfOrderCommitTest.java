package com.github.highcumontoa.taskqueueruntimejava.tests;

import com.github.highcumontoa.taskqueueruntimejava.backend.LocalFileQueueBackend;
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
 * 乱序提交的位点安全：并行消费下后到的消息先确认时，
 * 组位点（连续水位）不得跳过尚未完成的更早消息；
 * 重启后从最早未完成消息继续，已确认的不重复、中间的不跳过。
 */
class OutOfOrderCommitTest {

    private static final Logger log = LoggerFactory.getLogger(OutOfOrderCommitTest.class);
    private static final String T = "ooo-events";
    private static final String G = "workers";

    private RuntimeTestSupport.Env memoryEnv() {
        var env = RuntimeTestSupport.memoryRuntime();
        env.runtime().createTopic(ADMIN, T,
                new TopicConfig(100, null, Duration.ofSeconds(1), Duration.ofMinutes(10),
                        com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1));
        env.runtime().createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env, T);
        return env;
    }

    private List<Delivery> pollN(TaskQueueRuntime rt, int n) {
        List<Delivery> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Delivery d = rt.poll(CONSUMER, T, G);
            assertNotNull(d, "应能取到第 " + i + " 条");
            out.add(d);
        }
        return out;
    }

    @Test
    void outOfOrderCommits_watermarkNeverSkipsUnfinished() {
        var env = memoryEnv();
        TaskQueueRuntime rt = env.runtime();
        for (int i = 0; i < 5; i++) {
            rt.produce(PRODUCER, T, "m" + i, null);
        }
        // 并行消费者同时取走 0..4，处理快慢不同导致确认乱序到达。
        List<Delivery> ds = pollN(rt, 5);

        // 后到的 offset 2 先确认：水位不能越过未完成的 0/1。
        rt.commit(CONSUMER, T, G, ds.get(2).getDeliveryId());
        assertEquals(-1, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "offset 0/1 未完成时水位不得前进");
        rt.commit(CONSUMER, T, G, ds.get(4).getDeliveryId());
        assertEquals(-1, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "乱序确认不得让水位跳过空洞");
        log.info("out-of-order commits held watermark committed={} committedOffsets=2,4",
                rt.offsetOf(CONSUMER, T, G).getCommitted());

        // 补齐 0：水位连续推进到 2（0,1?——1 尚未确认，只能到 0）。
        rt.commit(CONSUMER, T, G, ds.get(0).getDeliveryId());
        assertEquals(0, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "offset 1 未完成，水位只能推进到 0");
        // 补齐 1：0,1,2 连续完成，水位一次越过已确认的 2。
        rt.commit(CONSUMER, T, G, ds.get(1).getDeliveryId());
        assertEquals(2, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "连续水位应越过乱序中已确认的 2");
        // 补齐 3：3,4 连续，水位到 4。
        rt.commit(CONSUMER, T, G, ds.get(3).getDeliveryId());
        assertEquals(4, rt.offsetOf(CONSUMER, T, G).getCommitted());
        log.info("watermark caught up group={} committed={}", G,
                rt.offsetOf(CONSUMER, T, G).getCommitted());
    }

    @Test
    void restart_afterOutOfOrderCommits_resumesFromEarliestUnfinished() throws Exception {
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("queue-ooo-");
        var backend = new LocalFileQueueBackend(dir);
        var env1 = RuntimeTestSupport.newRuntime(backend);
        env1.runtime().createTopic(ADMIN, T,
                new TopicConfig(100, null, Duration.ofSeconds(1), Duration.ofMinutes(10),
                        com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1));
        env1.runtime().createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env1, T);
        TaskQueueRuntime rt1 = env1.runtime();
        for (int i = 0; i < 5; i++) {
            rt1.produce(PRODUCER, T, "m" + i, null);
        }
        List<Delivery> ds = pollN(rt1, 5);
        // 乱序确认 1、3 后进程退出；0、2、4 仍在途。
        rt1.commit(CONSUMER, T, G, ds.get(1).getDeliveryId());
        rt1.commit(CONSUMER, T, G, ds.get(3).getDeliveryId());
        assertEquals(-1, rt1.offsetOf(CONSUMER, T, G).getCommitted());
        log.info("shutdown after out-of-order commits offsets=1,3 committed={}",
                rt1.offsetOf(CONSUMER, T, G).getCommitted());
        rt1.close(Duration.ofSeconds(1));

        // 重启恢复：水位不跳变；在途被安全交还，从最早未完成（offset 0）继续。
        var backend2 = new LocalFileQueueBackend(dir);
        backend2.recover();
        var env2 = RuntimeTestSupport.newRuntime(backend2);
        RuntimeTestSupport.grantTopic(env2, T);
        TaskQueueRuntime rt2 = env2.runtime();
        assertEquals(-1, rt2.offsetOf(CONSUMER, T, G).getCommitted(),
                "重启后水位不得跳变");

        List<Long> redelivered = new ArrayList<>();
        List<Delivery> pending = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Delivery d = rt2.poll(CONSUMER, T, G);
            assertNotNull(d, "重启后应继续投递第 " + i + " 条未完成消息");
            redelivered.add(d.getOffset());
            pending.add(d);
        }
        assertEquals(List.of(0L, 2L, 4L), redelivered,
                "重启后必须按序投递未完成消息，已确认的 1、3 不得重复");
        assertNull(rt2.poll(CONSUMER, T, G), "已确认消息不得重复投递");
        log.info("redelivered after restart offsets={} reasons={}", redelivered,
                pending.stream().map(Delivery::getReason).toList());

        // 确认 0、2 后水位连续推进到 4（1、3 重启前已确认，不得重复处理）。
        rt2.commit(CONSUMER, T, G, pending.get(0).getDeliveryId());
        assertEquals(1, rt2.offsetOf(CONSUMER, T, G).getCommitted(),
                "0 确认后水位越过此前已确认的 1");
        rt2.commit(CONSUMER, T, G, pending.get(1).getDeliveryId());
        assertEquals(3, rt2.offsetOf(CONSUMER, T, G).getCommitted());
        rt2.commit(CONSUMER, T, G, pending.get(2).getDeliveryId());
        assertEquals(4, rt2.offsetOf(CONSUMER, T, G).getCommitted());

        // 显式重放仍能覆盖这段区间：回到 -1，全部重新投递且原因可区分。
        rt2.replay(ADMIN, T, G, -1);
        Delivery replayed = rt2.poll(CONSUMER, T, G);
        assertNotNull(replayed);
        assertEquals(0, replayed.getOffset());
        assertEquals(DeliveryReason.REPLAY, replayed.getReason());
        log.info("replay after restart group={} firstOffset={} reason={}",
                G, replayed.getOffset(), replayed.getReason());
        rt2.close(Duration.ofSeconds(1));
    }
}
