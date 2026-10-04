package com.github.highcumontoa.taskqueueruntimejava.tests;

import com.github.highcumontoa.taskqueueruntimejava.error.QueueException;
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

/** 位点提交、重启恢复不跳变、非法回退/跨组提交拒绝测试。 */
class OffsetAndReplayTest {

    private static final Logger log = LoggerFactory.getLogger(OffsetAndReplayTest.class);
    private static final String T = "events";
    private static final String G1 = "g1";
    private static final String G2 = "g2";

    private RuntimeTestSupport.Env env() {
        var env = RuntimeTestSupport.memoryRuntime();
        env.runtime().createTopic(ADMIN, T,
                new TopicConfig(100, null, Duration.ofSeconds(1), Duration.ofSeconds(30),
                        com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1));
        env.runtime().createGroup(ADMIN, T, G1);
        env.runtime().createGroup(ADMIN, T, G2);
        RuntimeTestSupport.grantTopic(env, T);
        return env;
    }

    @Test
    void committedOffset_movesForward_monotonically() {
        var env = env();
        TaskQueueRuntime rt = env.runtime();
        rt.produce(PRODUCER, T, "1", null);
        rt.produce(PRODUCER, T, "2", null);

        Delivery d0 = rt.poll(CONSUMER, T, G1);
        assertEquals(0, d0.getOffset());
        var info0 = rt.offsetOf(CONSUMER, T, G1);
        assertEquals(-1, info0.getCommitted(), "初始位点为 -1");

        rt.commit(CONSUMER, T, G1, d0.getDeliveryId());
        var info1 = rt.offsetOf(CONSUMER, T, G1);
        assertEquals(0, info1.getCommitted());
        log.info("offset advanced group={} messageId={} committed={}",
                G1, d0.getMessageId(), info1.getCommitted());

        // 跳过未提交消息提交 offset 2 时，位点只按被提交消息推进（此处顺序消费，不会越位）。
        Delivery d1 = rt.poll(CONSUMER, T, G1);
        assertEquals(1, d1.getOffset());
        rt.commit(CONSUMER, T, G1, d1.getDeliveryId());
        assertEquals(1, rt.offsetOf(CONSUMER, T, G1).getCommitted());
    }

    @Test
    void crossGroupCommit_isRejectedWithDistinctReason() {
        var env = env();
        TaskQueueRuntime rt = env.runtime();
        rt.produce(PRODUCER, T, "1", null);
        Delivery g1Delivery = rt.poll(CONSUMER, T, G1);
        Delivery g2Delivery = rt.poll(CONSUMER, T, G2);
        log.info("groups receive independent deliveries g1.deliveryId={} g2.deliveryId={} offset={}",
                g1Delivery.getDeliveryId(), g2Delivery.getDeliveryId(), g1Delivery.getOffset());

        // 拿 g2 的投递去 g1 提交 -> 跨组拒绝
        QueueException ex = assertThrows(QueueException.class,
                () -> rt.commit(CONSUMER, T, G1, g2Delivery.getDeliveryId()));
        assertEquals(com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode.CROSS_GROUP_COMMIT_REJECTED,
                ex.getCode(), "跨组提交必须以独立错误码拒绝");
        log.warn("cross-group commit rejected group={} deliveryOfGroup={} code={} reason={}",
                G1, G2, ex.getCode(), ex.getMessage());

        // 同组正常提交不受影响
        var ok = rt.commit(CONSUMER, T, G1, g1Delivery.getDeliveryId());
        assertEquals(com.github.highcumontoa.taskqueueruntimejava.model.CommitOutcome.COMMITTED,
                ok.getOutcome(), "拒绝越权提交不得影响合法提交");
    }

    @Test
    void illegalRollback_andForwardJump_areRejectedDistinctly() {
        var env = env();
        TaskQueueRuntime rt = env.runtime();
        rt.produce(PRODUCER, T, "1", null);
        rt.produce(PRODUCER, T, "2", null);
        Delivery d0 = rt.poll(CONSUMER, T, G1);
        rt.commit(CONSUMER, T, G1, d0.getDeliveryId()); // committed=0

        // 目标位点超过本组最高已确认位点 -> 区间内没有已确认消息可重放（无操作），
        // 以 OFFSET_ROLLBACK_REJECTED 与成功重放明确区分。
        QueueException forward = assertThrows(QueueException.class,
                () -> rt.replay(ADMIN, T, G1, 5));
        assertEquals(com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode.OFFSET_ROLLBACK_REJECTED,
                forward.getCode());
        // 非法位点（< -1）-> BAD_REQUEST（参数错误可区分）。
        QueueException illegal = assertThrows(QueueException.class,
                () -> rt.replay(ADMIN, T, G1, -2));
        assertEquals(com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode.BAD_REQUEST,
                illegal.getCode());
        // 回退到当前位点（无变化）-> OFFSET_ROLLBACK_REJECTED（非法回退可区分）
        QueueException same = assertThrows(QueueException.class,
                () -> rt.replay(ADMIN, T, G1, 0));
        assertEquals(com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode.OFFSET_ROLLBACK_REJECTED,
                same.getCode());
        log.warn("illegal rollbacks rejected group={} noOpCode={} badRequestCode={}",
                G1, forward.getCode(), illegal.getCode());

        // 合法显式回退到 -1：消息以 REPLAY 原因重新投递
        var info = rt.replay(ADMIN, T, G1, -1);
        assertEquals(-1, info.getCommitted());
        assertEquals(true, info.isReplayed());
        assertEquals(1, info.getResetCount(), "offset=0 一条已确认消息被重置");
        assertEquals(0, info.getReplayHigh());
        Delivery redelivered = rt.poll(CONSUMER, T, G1);
        assertNotNull(redelivered);
        assertEquals(DeliveryReason.REPLAY, redelivered.getReason(),
                "重放投递原因必须可区分");
        log.info("replay delivery group={} messageId={} offset={} attempt={} reason={}",
                G1, redelivered.getMessageId(), redelivered.getOffset(),
                redelivered.getAttempt(), redelivered.getReason());
    }

    @Test
    void restart_doesNotLoseOrSkipOffsets_withFileBackend() throws Exception {
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("queue-restart-");
        var backend = new com.github.highcumontoa.taskqueueruntimejava.backend.LocalFileQueueBackend(dir);
        var env1 = RuntimeTestSupport.newRuntime(backend);
        env1.runtime().createTopic(ADMIN, T,
                new TopicConfig(100, null, Duration.ofSeconds(1), Duration.ofMinutes(10),
                        com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1));
        env1.runtime().createGroup(ADMIN, T, G1);
        RuntimeTestSupport.grantTopic(env1, T);
        env1.runtime().produce(PRODUCER, T, "1", null);
        env1.runtime().produce(PRODUCER, T, "2", null);
        Delivery d0 = env1.runtime().poll(CONSUMER, T, G1);
        env1.runtime().commit(CONSUMER, T, G1, d0.getDeliveryId());
        env1.runtime().close(Duration.ofSeconds(1));
        log.info("shutdown after commit offset=0 messageId={} group={}", d0.getMessageId(), G1);

        // 新进程：从磁盘恢复，位点必须仍是 0，下一条是 offset 1，不跳变。
        var backend2 = new com.github.highcumontoa.taskqueueruntimejava.backend.LocalFileQueueBackend(dir);
        backend2.recover();
        var env2 = RuntimeTestSupport.newRuntime(backend2);
        // 授权信息不随消息存储持久化（安全模型），重启后由外部重新授权。
        RuntimeTestSupport.grantTopic(env2, T);
        var info = env2.runtime().offsetOf(CONSUMER, T, G1);
        assertEquals(0, info.getCommitted(), "重启后已提交位点不得跳变");
        Delivery next = env2.runtime().poll(CONSUMER, T, G1);
        assertNotNull(next);
        assertEquals(1, next.getOffset(), "重启后不得越位，必须从 offset 1 继续");
        log.info("recovered group={} committed={} nextOffset={} messageId={}",
                G1, info.getCommitted(), next.getOffset(), next.getMessageId());
        env2.runtime().close(Duration.ofSeconds(1));
    }
}
