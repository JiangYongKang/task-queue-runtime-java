package com.github.highcumontoa.taskqueueruntimejava.tests.queue;

import com.github.highcumontoa.taskqueueruntimejava.queue.config.QueueRuntimeProperties;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.BackpressurePolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.DeliveryReason;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueRequest;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.ReceiveResult;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.RetryPolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.TopicSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.QueueRuntimeException;
import com.github.highcumontoa.taskqueueruntimejava.queue.runtime.TaskQueueRuntime;
import com.github.highcumontoa.taskqueueruntimejava.queue.storage.LocalFileStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 重点：位点持久化、进程重启恢复、位点回退/前进重置拒绝、
 * 跨分组提交拒绝、位点越界拒绝，且原因可区分。
 */
class OffsetAndRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(OffsetAndRecoveryTest.class);
    private static final String ADMIN = TaskQueueRuntime.DEFAULT_ADMIN_TOKEN;

    private QueueRuntimeProperties props(Path dir) {
        return new QueueRuntimeProperties(QueueRuntimeProperties.BackendType.LOCAL_FILE,
                dir.toString(), 10_000, BackpressurePolicy.REJECT,
                60_000L, 1_000L, 100L, 10_000);
    }

    private void seed(TaskQueueRuntime rt) {
        rt.createTopic(ADMIN, "events", new TopicSettings(100, BackpressurePolicy.REJECT, 10));
        rt.createGroup(ADMIN, "events", "g1", new GroupSettings(60_000L, RetryPolicy.none()));
        rt.createGroup(ADMIN, "events", "g2", new GroupSettings(60_000L, RetryPolicy.none()));
        for (int i = 0; i < 3; i++) {
            rt.produce(ADMIN, new EnqueueRequest("events", ("m" + i).getBytes(), null, null, null));
        }
    }

    private Delivery consumeAndCommit(TaskQueueRuntime rt, String group) {
        ReceiveResult rr = rt.receive(ADMIN, "events", group, "c", 1, 500L);
        Delivery d = rr.deliveries().get(0);
        rt.commit(ADMIN, "events", group, d.lease().deliveryToken(), null);
        return d;
    }

    @Test
    void offsetsAndInFlightLeasesSurviveRestart(@TempDir Path dir) {
        try (TaskQueueRuntime rt = new TaskQueueRuntime(new LocalFileStorage(dir), props(dir))) {
            seed(rt);
            consumeAndCommit(rt, "g1");
            consumeAndCommit(rt, "g1");
            // g2 只取走不提交，模拟在途
            Delivery inflight = rt.receive(ADMIN, "events", "g2", "c", 1, 500L).deliveries().get(0);
            log.info("[RESTART] pre-restart g1={} g2-inflight-offset={}",
                    rt.committedOffset(ADMIN, "events", "g1"), inflight.lease().offset());
            assertEquals(2L, rt.committedOffset(ADMIN, "events", "g1"));
            assertEquals(0L, rt.committedOffset(ADMIN, "events", "g2"));
        }

        // 重启：位点不跳变；g1 只能从 offset=2 继续。
        // g2 在途消息在上一进程优雅关闭时已被安全交还，重启后必须以
        // REDISPATCH_AFTER_SHUTDOWN 重新投递（而不是丢失）。
        try (TaskQueueRuntime rt2 = new TaskQueueRuntime(new LocalFileStorage(dir), props(dir))) {
            assertEquals(2L, rt2.committedOffset(ADMIN, "events", "g1"));
            assertEquals(0L, rt2.committedOffset(ADMIN, "events", "g2"));
            Delivery next = rt2.receive(ADMIN, "events", "g1", "c", 1, 500L).deliveries().get(0);
            log.info("[RESTART] post-restart g1 next offset={} messageId={}",
                    next.lease().offset(), next.message().messageId());
            assertEquals(2L, next.lease().offset());

            Delivery g2redelivery = rt2.receive(ADMIN, "events", "g2", "c2", 1, 500L).deliveries().get(0);
            log.info("[RESTART] g2 redelivered offset={} messageId={} reason={}",
                    g2redelivery.lease().offset(), g2redelivery.message().messageId(),
                    g2redelivery.lease().reason());
            assertEquals(0L, g2redelivery.lease().offset());
            assertEquals(DeliveryReason.REDISPATCH_AFTER_SHUTDOWN, g2redelivery.lease().reason());
        }
    }

    @Test
    void offsetRollbackCommitForwardResetAndCrossGroupAreRejectedWithDistinctReasons(@TempDir Path dir) {
        try (TaskQueueRuntime rt = new TaskQueueRuntime(new LocalFileStorage(dir), props(dir))) {
            seed(rt);
            consumeAndCommit(rt, "g1"); // g1 提交到 1
            Delivery g1 = rt.receive(ADMIN, "events", "g1", "c", 1, 500L).deliveries().get(0);
            Delivery g2 = rt.receive(ADMIN, "events", "g2", "c", 1, 500L).deliveries().get(0);

            // 1) 跨分组提交：g2 的令牌提到 g1
            QueueRuntimeException cross = assertThrows(QueueRuntimeException.class,
                    () -> rt.commit(ADMIN, "events", "g1", g2.lease().deliveryToken(), null));
            log.info("[REJECT] cross-group code={} msg={}", cross.errorCode(), cross.getMessage());
            assertEquals(ErrorCode.CROSS_GROUP_COMMIT_REJECTED, cross.errorCode());

            // 2) 管理员回退位点后，回退点之后的旧租约作废 -> UNKNOWN_DELIVERY_TOKEN
            rt.resetOffset(ADMIN, "events", "g1", 0L);
            QueueRuntimeException stale = assertThrows(QueueRuntimeException.class,
                    () -> rt.commit(ADMIN, "events", "g1", g1.lease().deliveryToken(), null));
            log.info("[REJECT] stale-token-after-reset code={} msg={}", stale.errorCode(), stale.getMessage());
            assertEquals(ErrorCode.UNKNOWN_DELIVERY_TOKEN, stale.errorCode());

            // 重放：offset 0 被重新消费并提交，committedOffset 前进到 1
            Delivery replay0 = rt.receive(ADMIN, "events", "g1", "c", 1, 500L).deliveries().get(0);
            assertEquals(0L, replay0.lease().offset());
            assertEquals(DeliveryReason.REPLAY, replay0.lease().reason());
            rt.commit(ADMIN, "events", "g1", replay0.lease().deliveryToken(), null);
            assertEquals(1L, rt.committedOffset(ADMIN, "events", "g1"));

            // 此时已提交位点为 1。伪造一个结构合法、位点为 0 的历史令牌（模拟客户端缓存的旧令牌）
            // 提交老位点 -> OFFSET_ROLLBACK_REJECTED，与 UNKNOWN_DELIVERY_TOKEN 明确区分。
            String forgedOldToken = "dt_events.g1.0.deadbeefdeadbeef";
            QueueRuntimeException rollback = assertThrows(QueueRuntimeException.class,
                    () -> rt.commit(ADMIN, "events", "g1", forgedOldToken, null));
            log.info("[REJECT] rollback code={} msg={}", rollback.errorCode(), rollback.getMessage());
            assertEquals(ErrorCode.OFFSET_ROLLBACK_REJECTED, rollback.errorCode());

            // 3) 前进重置（目标不小于当前 committedOffset）被拒绝
            QueueRuntimeException forward = assertThrows(QueueRuntimeException.class,
                    () -> rt.resetOffset(ADMIN, "events", "g1", 1L));
            log.info("[REJECT] forward-reset code={} msg={}", forward.errorCode(), forward.getMessage());
            assertEquals(ErrorCode.ILLEGAL_RESET_TARGET, forward.errorCode());

            // 4) 越界重置（超过日志末尾）被拒绝，且不改变位点
            QueueRuntimeException oob = assertThrows(QueueRuntimeException.class,
                    () -> rt.resetOffset(ADMIN, "events", "g1", 999L));
            log.info("[REJECT] out-of-range code={} msg={}", oob.errorCode(), oob.getMessage());
            assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, oob.errorCode());
            assertEquals(1L, rt.committedOffset(ADMIN, "events", "g1"));
        }
    }

    /** 回退到带死信的位点之前，旧死信记录必须清除，以便重放后重新处置。 */
    @Test
    void resetClearsDeadLettersForReplay(@TempDir Path dir) {
        try (TaskQueueRuntime rt = new TaskQueueRuntime(new LocalFileStorage(dir), props(dir))) {
            rt.createTopic(ADMIN, "events", new TopicSettings(100, BackpressurePolicy.REJECT, 10));
            rt.createGroup(ADMIN, "events", "g", new GroupSettings(60_000L, RetryPolicy.none()));
            // 生产两条：保证 offset 0 进入死信、位点推进到 1 后，offset 0 仍保留在日志中可重放
            rt.produce(ADMIN, new EnqueueRequest("events", "m0".getBytes(), null, null, null));
            rt.produce(ADMIN, new EnqueueRequest("events", "m1".getBytes(), null, null, null));
            Delivery d = rt.receive(ADMIN, "events", "g", "c", 1, 500L).deliveries().get(0);
            rt.nack(ADMIN, "events", "g", d.lease().deliveryToken(), "E", "fatal", true);
            assertEquals(1, rt.deadLetters(ADMIN, "events", "g").size());

            rt.resetOffset(ADMIN, "events", "g", 0L);
            assertTrue(rt.deadLetters(ADMIN, "events", "g").isEmpty(), "回退后死信应清除");
            Delivery replay = rt.receive(ADMIN, "events", "g", "c", 1, 500L).deliveries().get(0);
            assertEquals(0L, replay.lease().offset());
            assertEquals(DeliveryReason.REPLAY, replay.lease().reason());
            log.info("[REPLAY-DLQ] group=g offset={} messageId={} reason={}",
                    replay.lease().offset(), replay.message().messageId(), replay.lease().reason());
        }
    }
}
