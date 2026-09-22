package com.github.highcumontoa.taskqueueruntimejava.tests.queue;

import com.github.highcumontoa.taskqueueruntimejava.queue.config.QueueRuntimeProperties;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.BackpressurePolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.DeadLetterReason;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.DeliveryReason;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueRequest;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.MessageState;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.NackOutcome;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.NackReceipt;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.ReceiveResult;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.RetryPolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.TopicSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.runtime.RuntimeFactory;
import com.github.highcumontoa.taskqueueruntimejava.queue.runtime.TaskQueueRuntime;
import com.github.highcumontoa.taskqueueruntimejava.queue.storage.QueueStorage;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 重点：退避重试有次数上限，超限进入 RETRY_EXHAUSTED 死信；
 * 不可重试错误立即进入 NON_RETRYABLE 死信；死信可查询且位点继续前进。
 */
class RetryAndDeadLetterTest {

    private static final Logger log = LoggerFactory.getLogger(RetryAndDeadLetterTest.class);
    private static final String ADMIN = TaskQueueRuntime.DEFAULT_ADMIN_TOKEN;

    private TaskQueueRuntime newRuntime() {
        QueueRuntimeProperties props = QueueRuntimeProperties.defaults();
        return new TaskQueueRuntime(RuntimeFactory.createStorage(props), props);
    }

    private Delivery poll(TaskQueueRuntime rt, String topic, String group) {
        ReceiveResult rr = rt.receive(ADMIN, topic, group, "c", 1, 2_000L);
        assertFalse(rr.deliveries().isEmpty(), "期望拿到投递");
        return rr.deliveries().get(0);
    }

    @Test
    void retriesUseBackoffAndRetryExhaustedEntersDeadLetter() {
        // maxAttempts=3：首次 + 2 次重试，退避 50ms -> 100ms
        RetryPolicy policy = new RetryPolicy(3, 50L, 2.0, 5_000L);
        try (TaskQueueRuntime rt = newRuntime()) {
            rt.createTopic(ADMIN, "jobs", new TopicSettings(100, BackpressurePolicy.REJECT));
            rt.createGroup(ADMIN, "jobs", "workers", new GroupSettings(60_000L, policy));
            rt.produce(ADMIN, new EnqueueRequest("jobs", "task".getBytes(), null, null, null));

            long start = System.currentTimeMillis();
            long[] expectedDelay = {50L, 100L};

            for (int attempt = 1; attempt <= 2; attempt++) {
                Delivery d = poll(rt, "jobs", "workers");
                log.info("[RETRY] group=workers offset={} messageId={} attempt={} reason={}",
                        d.lease().offset(), d.message().messageId(), d.lease().attempt(), d.lease().reason());
                assertEquals(attempt, d.lease().attempt());
                assertEquals(attempt == 1 ? DeliveryReason.INITIAL : DeliveryReason.RETRY_AFTER_NACK,
                        d.lease().reason());
                long before = System.currentTimeMillis();
                NackReceipt nack = rt.nack(ADMIN, "jobs", "workers",
                        d.lease().deliveryToken(), "E_TRANSIENT", "boom", true);
                long elapsed = System.currentTimeMillis() - before;
                log.info("[NACK] group=workers offset={} attempt={} outcome={} nextAttempt={} waitElapsedMs={}",
                        d.lease().offset(), attempt, nack.outcome(), nack.nextAttempt(), elapsed);
                assertEquals(NackOutcome.RETRY_SCHEDULED, nack.outcome());
                assertEquals(attempt + 1, nack.nextAttempt());
            }

            // 第 3 次投递（最后一次机会）仍失败 -> 死信 RETRY_EXHAUSTED
            Delivery last = poll(rt, "jobs", "workers");
            assertEquals(3, last.lease().attempt());
            long elapsedTotal = System.currentTimeMillis() - start;
            NackReceipt dead = rt.nack(ADMIN, "jobs", "workers",
                    last.lease().deliveryToken(), "E_TRANSIENT", "still boom", true);
            log.info("[DEAD] group=workers offset={} messageId={} attempts={} outcome={} reason={} totalElapsedMs={}",
                    last.lease().offset(), last.message().messageId(),
                    dead.deadLetterRecord().attempts(), dead.outcome(),
                    dead.deadLetterRecord().reason(), elapsedTotal);
            assertEquals(NackOutcome.DEAD_LETTER, dead.outcome());
            assertEquals(DeadLetterReason.RETRY_EXHAUSTED, dead.deadLetterRecord().reason());
            assertEquals(3, dead.deadLetterRecord().attempts());

            List<?> dlq = rt.deadLetters(ADMIN, "jobs", "workers");
            assertEquals(1, dlq.size());
            // 退避必须实际发生（50+100=150ms 下界，留调度余量）
            assertTrue(elapsedTotal >= 140L, "退避应实际等待，实际=" + elapsedTotal);
            // 死信是终态，位点推进，消息不再投递
            assertEquals(1L, rt.committedOffset(ADMIN, "jobs", "workers"));
            assertEquals(MessageState.DEAD, rt.messageState(ADMIN, "jobs", "workers", 0L));
            ReceiveResult empty = rt.receive(ADMIN, "jobs", "workers", "c", 1, 200L);
            assertTrue(empty.deliveries().isEmpty());
        }
    }

    @Test
    void nonRetryableErrorGoesStraightToDeadLetter() {
        RetryPolicy policy = new RetryPolicy(5, 50L, 2.0, 5_000L);
        try (TaskQueueRuntime rt = newRuntime()) {
            rt.createTopic(ADMIN, "jobs", new TopicSettings(100, BackpressurePolicy.REJECT));
            rt.createGroup(ADMIN, "jobs", "w2", new GroupSettings(60_000L, policy));
            rt.produce(ADMIN, new EnqueueRequest("jobs", "bad".getBytes(), null, null, null));
            Delivery d = poll(rt, "jobs", "w2");
            NackReceipt dead = rt.nack(ADMIN, "jobs", "w2",
                    d.lease().deliveryToken(), "E_INVALID_PAYLOAD", "cannot parse", false);
            log.info("[NON-RETRYABLE] group=w2 offset={} messageId={} attempt={} reason={}",
                    d.lease().offset(), d.message().messageId(), d.lease().attempt(),
                    dead.deadLetterRecord().reason());
            assertEquals(NackOutcome.DEAD_LETTER, dead.outcome());
            assertEquals(DeadLetterReason.NON_RETRYABLE, dead.deadLetterRecord().reason());
            assertEquals(1, rt.deadLetters(ADMIN, "jobs", "w2").size());
            assertEquals(1L, rt.committedOffset(ADMIN, "jobs", "w2"));
        }
    }
}
