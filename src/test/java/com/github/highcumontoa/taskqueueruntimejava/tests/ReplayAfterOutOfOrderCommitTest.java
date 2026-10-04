package com.github.highcumontoa.taskqueueruntimejava.tests;

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
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 乱序确认与显式重放的组合安全：
 * 后面的消息先确认、前面的消息仍在处理时，已确认消息的位点会“超在”
 * 对外水位线之前。对更早位点发起显式重放（含从头重放）时，
 * 这些已确认消息也必须被重新投递，对外进度必须与实际重新投递一致，
 * 任何一条都不得被静默跳过。
 */
class ReplayAfterOutOfOrderCommitTest {

    private static final Logger log = LoggerFactory.getLogger(ReplayAfterOutOfOrderCommitTest.class);
    private static final String T = "replay-events";
    private static final String G = "workers";

    private RuntimeTestSupport.Env env() {
        var env = RuntimeTestSupport.memoryRuntime();
        env.runtime().createTopic(ADMIN, T,
                new TopicConfig(100, null, Duration.ofSeconds(1), Duration.ofSeconds(30),
                        com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1));
        env.runtime().createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env, T);
        return env;
    }

    @Test
    void replayFromHead_afterOutOfOrderCommit_redeliversCommittedAheadOfWatermark() {
        var env = env();
        TaskQueueRuntime rt = env.runtime();
        for (int i = 0; i < 3; i++) {
            rt.produce(PRODUCER, T, "m" + i, null);
        }
        Delivery d0 = rt.poll(CONSUMER, T, G);
        Delivery d1 = rt.poll(CONSUMER, T, G);
        Delivery d2 = rt.poll(CONSUMER, T, G);

        // 乱序确认：0 与 2 已确认，1 仍在处理中。水位线停在 0，
        // offset=2 的确认“超在”对外进度之前。
        rt.commit(CONSUMER, T, G, d0.getDeliveryId());
        rt.commit(CONSUMER, T, G, d2.getDeliveryId());
        assertEquals(0, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "缺口未补齐，水位线不得越过仍在处理中的 offset=1");
        log.info("乱序确认后水位线 group={} committed={} (offset=2 已确认但超在水位线之前)",
                G, rt.offsetOf(CONSUMER, T, G).getCommitted());

        // 从头重放：区间内所有已确认消息（含超在水位线之前的 offset=2）都必须重新投递。
        var replayed = rt.replay(ADMIN, T, G, -1);
        assertEquals(-1, replayed.getCommitted(), "重放后对外位点必须回到目标位点");

        Delivery r0 = rt.poll(CONSUMER, T, G);
        assertNotNull(r0, "重放后 offset=0 必须重新投递");
        assertEquals(0, r0.getOffset());
        assertEquals(DeliveryReason.REPLAY, r0.getReason());
        rt.commit(CONSUMER, T, G, r0.getDeliveryId());
        assertEquals(0, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "offset=1 仍在处理，水位线只能推进到 0");

        // 关键断言：offset=2 此前已确认且位点超在水位线之前，重放必须把它重新投递，
        // 不得因为它“排在对外进度之前”就被静默跳过。
        Delivery r2 = rt.poll(CONSUMER, T, G);
        assertNotNull(r2, "重放不得漏投已确认但超在水位线之前的消息（offset=2）");
        assertEquals(2, r2.getOffset());
        assertEquals(DeliveryReason.REPLAY, r2.getReason());
        rt.commit(CONSUMER, T, G, r2.getDeliveryId());
        assertEquals(0, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "缺口 offset=1 未补齐，水位线不得越位");

        // 缺口 offset=1 因可见性超时被回收重投，补齐后水位线一次性越过 2。
        env.clock().advance(Duration.ofSeconds(31));
        rt.reclaimExpired(T);
        Delivery r1 = rt.poll(CONSUMER, T, G);
        assertNotNull(r1, "缺口消息超时后必须重新投递");
        assertEquals(1, r1.getOffset());
        assertEquals(DeliveryReason.VISIBILITY_TIMEOUT, r1.getReason());
        rt.commit(CONSUMER, T, G, r1.getDeliveryId());

        assertEquals(2, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "重放轮次全部重新确认后，对外进度必须与实际重新投递一致");
        assertNull(rt.poll(CONSUMER, T, G), "全部重新投递完成后不应再有投递");
        log.info("乱序确认+从头重放完成 group={} committed={} 重投区间=[0,2] 无静默跳过",
                G, rt.offsetOf(CONSUMER, T, G).getCommitted());
    }

    @Test
    void replay_interleavedWithInflightCommit_watermarkMonotonicAndNoDuplicateEffect() {
        var env = env();
        TaskQueueRuntime rt = env.runtime();
        for (int i = 0; i < 4; i++) {
            rt.produce(PRODUCER, T, "m" + i, null);
        }
        Delivery d0 = rt.poll(CONSUMER, T, G);
        Delivery d1 = rt.poll(CONSUMER, T, G);
        Delivery d2 = rt.poll(CONSUMER, T, G);
        Delivery d3 = rt.poll(CONSUMER, T, G);

        // 乱序确认：0、1、3 已确认，2 仍在处理中。水位线=1，offset=3 超在水位线之前。
        rt.commit(CONSUMER, T, G, d0.getDeliveryId());
        rt.commit(CONSUMER, T, G, d1.getDeliveryId());
        rt.commit(CONSUMER, T, G, d3.getDeliveryId());
        assertEquals(1, rt.offsetOf(CONSUMER, T, G).getCommitted());

        // 重放到 0：区间 (0, 3] 的已确认消息（1 和超在水位线之前的 3）被重置；
        // 仍在处理中的 offset=2 不属于重放范围，其在途投递保持有效。
        rt.replay(ADMIN, T, G, 0);
        assertEquals(0, rt.offsetOf(CONSUMER, T, G).getCommitted());

        // 重放进行中，offset=2 的在途处理完成并确认：水位线不得倒退，
        // 只能停在第一个未终结缺口（offset=1 待重投）之前。
        rt.commit(CONSUMER, T, G, d2.getDeliveryId());
        long afterInflightCommit = rt.offsetOf(CONSUMER, T, G).getCommitted();
        assertEquals(0, afterInflightCommit, "在途确认与重放交错时水位线不得越位也不得倒退");

        Delivery r1 = rt.poll(CONSUMER, T, G);
        assertNotNull(r1);
        assertEquals(1, r1.getOffset());
        assertEquals(DeliveryReason.REPLAY, r1.getReason());
        rt.commit(CONSUMER, T, G, r1.getDeliveryId());
        // 缺口 1 补齐后，水位线一次性越过此前已确认的 2，停在待重投的 3 之前。
        assertEquals(2, rt.offsetOf(CONSUMER, T, G).getCommitted());

        Delivery r3 = rt.poll(CONSUMER, T, G);
        assertNotNull(r3, "超在水位线之前的已确认消息 offset=3 必须被重放覆盖");
        assertEquals(3, r3.getOffset());
        assertEquals(DeliveryReason.REPLAY, r3.getReason());
        rt.commit(CONSUMER, T, G, r3.getDeliveryId());
        assertEquals(3, rt.offsetOf(CONSUMER, T, G).getCommitted());
        assertNull(rt.poll(CONSUMER, T, G),
                "offset=2 已通过在途确认生效，不得被重复投递；全部完成后应无投递");
        log.info("重放与在途确认交错完成 group={} committed={} 水位线序列 1->(replay)0->0->2->3 单调",
                G, rt.offsetOf(CONSUMER, T, G).getCommitted());
    }

    @Test
    void replay_thenRestart_recoveryKeepsReplayCoverageAndOffset() throws Exception {
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("queue-replay-restart-");
        var backend = new com.github.highcumontoa.taskqueueruntimejava.backend.LocalFileQueueBackend(dir);
        var env1 = RuntimeTestSupport.newRuntime(backend);
        env1.runtime().createTopic(ADMIN, T, new TopicConfig(100, null, Duration.ofSeconds(1),
                Duration.ofSeconds(30),
                com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1));
        env1.runtime().createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env1, T);
        for (int i = 0; i < 3; i++) {
            env1.runtime().produce(PRODUCER, T, "m" + i, null);
        }
        Delivery d0 = env1.runtime().poll(CONSUMER, T, G);
        Delivery d1 = env1.runtime().poll(CONSUMER, T, G);
        Delivery d2 = env1.runtime().poll(CONSUMER, T, G);
        // 乱序确认：0、2 已确认，1 仍在处理；随后从头重放。
        env1.runtime().commit(CONSUMER, T, G, d0.getDeliveryId());
        env1.runtime().commit(CONSUMER, T, G, d2.getDeliveryId());
        var replayInfo = env1.runtime().replay(ADMIN, T, G, -1);
        assertEquals(-1, replayInfo.getCommitted());
        log.info("重放后立刻重启 group={} committed={} (offset=0,2 待重投, offset=1 在途)",
                G, replayInfo.getCommitted());

        // 进程退出：在途的 offset=1 被安全交还；重放待投状态随快照持久化。
        env1.runtime().close(Duration.ZERO);

        // 重启恢复：位点保持 -1 不跳变；重放区间 [0,2] 一条不少地重新投递。
        var backend2 = new com.github.highcumontoa.taskqueueruntimejava.backend.LocalFileQueueBackend(dir);
        backend2.recover();
        var env2 = RuntimeTestSupport.newRuntime(backend2);
        RuntimeTestSupport.grantTopic(env2, T);
        TaskQueueRuntime rt2 = env2.runtime();
        assertEquals(-1, rt2.offsetOf(CONSUMER, T, G).getCommitted(),
                "重启后位点不得跳变");

        Delivery r0 = rt2.poll(CONSUMER, T, G);
        assertNotNull(r0, "重启后重放区间第一条必须仍在待投");
        assertEquals(0, r0.getOffset());
        assertEquals(DeliveryReason.REPLAY, r0.getReason());
        rt2.commit(CONSUMER, T, G, r0.getDeliveryId());
        assertEquals(0, rt2.offsetOf(CONSUMER, T, G).getCommitted());

        Delivery r1 = rt2.poll(CONSUMER, T, G);
        assertNotNull(r1);
        assertEquals(1, r1.getOffset());
        assertEquals(DeliveryReason.VISIBILITY_TIMEOUT, r1.getReason(),
                "关闭时交还的在途消息以超时重投原因重新投递");
        rt2.commit(CONSUMER, T, G, r1.getDeliveryId());
        assertEquals(1, rt2.offsetOf(CONSUMER, T, G).getCommitted());

        Delivery r2 = rt2.poll(CONSUMER, T, G);
        assertNotNull(r2, "重启后不得漏投超在水位线之前的已确认消息（offset=2）");
        assertEquals(2, r2.getOffset());
        assertEquals(DeliveryReason.REPLAY, r2.getReason());
        rt2.commit(CONSUMER, T, G, r2.getDeliveryId());

        assertEquals(2, rt2.offsetOf(CONSUMER, T, G).getCommitted(),
                "重放轮次在重启后完成，对外进度必须与实际重新投递一致");
        assertNull(rt2.poll(CONSUMER, T, G), "全部重新投递完成后不应再有投递");
        log.info("重放后重启恢复完成 group={} committed={} 重投区间=[0,2] 无静默跳过",
                G, rt2.offsetOf(CONSUMER, T, G).getCommitted());
        rt2.close(Duration.ZERO);
    }
}
