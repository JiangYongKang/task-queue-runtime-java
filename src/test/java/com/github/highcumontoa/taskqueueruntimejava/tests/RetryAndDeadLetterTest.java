package com.github.highcumontoa.taskqueueruntimejava.tests;

import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.error.QueueException;
import com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig;
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
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 失败重试、指数退避、重试上限与死信处置测试。 */
class RetryAndDeadLetterTest {

    private static final Logger log = LoggerFactory.getLogger(RetryAndDeadLetterTest.class);
    private static final String T = "payments";
    private static final String G = "workers";

    private RuntimeTestSupport.Env setup(int maxAttempts, Duration initial) {
        var env = RuntimeTestSupport.memoryRuntime();
        var backoff = new BackoffConfig(initial, 2.0, Duration.ofSeconds(10), maxAttempts);
        env.runtime().createTopic(ADMIN, T,
                new TopicConfig(100, null, Duration.ofSeconds(1), Duration.ofSeconds(30), backoff, 1));
        env.runtime().createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env, T);
        return env;
    }

    @Test
    void retriesUseBackoff_andRedeliveryReasonIsRETRY() {
        var env = setup(3, Duration.ofMillis(100));
        TaskQueueRuntime rt = env.runtime();
        var receipt = rt.produce(PRODUCER, T, "m", null);
        log.info("[case] messageId={} group={} offset={}", receipt.getMessageId(), G, receipt.getOffset());

        Delivery d1 = rt.poll(CONSUMER, T, G);
        var n1 = rt.nack(CONSUMER, T, G, d1.getDeliveryId(), "transient", true);
        assertEquals(TaskQueueRuntime.NackResult.Status.RETRY_SCHEDULED, n1.getStatus());
        assertEquals(100, n1.getRetryAfterMillis(), "首次失败退避=initial");
        log.info("nack#1 messageId={} group={} offset={} attempt={} retryAfterMs={}",
                d1.getMessageId(), G, d1.getOffset(), n1.getAttempts(), n1.getRetryAfterMillis());

        // 退避窗口内不可取
        env.clock().advance(Duration.ofMillis(99));
        assertNull(rt.poll(CONSUMER, T, G), "退避窗口内不得投递");
        env.clock().advance(Duration.ofMillis(2));
        Delivery d2 = rt.poll(CONSUMER, T, G);
        assertNotNull(d2);
        assertEquals(DeliveryReason.RETRY, d2.getReason(),
                "合法重试原因 RETRY 必须与超时重投 VISIBILITY_TIMEOUT 区分");
        assertEquals(2, d2.getAttempt());
        log.info("retry delivery messageId={} group={} offset={} attempt={} reason={}",
                d2.getMessageId(), G, d2.getOffset(), d2.getAttempt(), d2.getReason());

        var n2 = rt.nack(CONSUMER, T, G, d2.getDeliveryId(), "again", true);
        assertEquals(200, n2.getRetryAfterMillis(), "第二次失败退避翻倍");
        log.info("nack#2 messageId={} group={} attempt={} retryAfterMs={}",
                d2.getMessageId(), G, n2.getAttempts(), n2.getRetryAfterMillis());
    }

    @Test
    void retriesExhausted_entersQueryableDeadLetter() {
        var env = setup(2, Duration.ofMillis(10));
        TaskQueueRuntime rt = env.runtime();
        var receipt = rt.produce(PRODUCER, T, "m", null);

        Delivery d1 = rt.poll(CONSUMER, T, G);
        rt.nack(CONSUMER, T, G, d1.getDeliveryId(), "fail1", true);
        env.clock().advance(Duration.ofMillis(50));
        Delivery d2 = rt.poll(CONSUMER, T, G);
        var n2 = rt.nack(CONSUMER, T, G, d2.getDeliveryId(), "fail2", true);
        assertEquals(TaskQueueRuntime.NackResult.Status.DEAD_LETTER, n2.getStatus(),
                "达到最大失败次数必须进入死信，不得无限重试");
        assertEquals(DeadLetterCause(n2), com.github.highcumontoa.taskqueueruntimejava.model.DeadLetterRecord.Cause.RETRIES_EXHAUSTED);
        log.warn("dead letter messageId={} group={} offset={} attempts={} cause={}",
                n2.getDeadLetter().getMessageId(), G, n2.getDeadLetter().getOffset(),
                n2.getDeadLetter().getAttempts(), n2.getDeadLetter().getCause());

        var dead = rt.deadLetters(CONSUMER, T);
        assertEquals(1, dead.size(), "死信必须可查询，不能被静默丢弃");
        assertEquals(receipt.getMessageId(), dead.get(0).getMessageId());
        // 死信后不再被投递
        env.clock().advance(Duration.ofHours(1));
        rt.reclaimExpired(T);
        assertNull(rt.poll(CONSUMER, T, G), "死信消息不得再次投递");
    }

    private static com.github.highcumontoa.taskqueueruntimejava.model.DeadLetterRecord.Cause
    DeadLetterCause(TaskQueueRuntime.NackResult r) {
        return r.getDeadLetter().getCause();
    }

    @Test
    void nonRetryableError_entersDeadLetterImmediately() {
        var env = setup(5, Duration.ofMillis(10));
        TaskQueueRuntime rt = env.runtime();
        rt.produce(PRODUCER, T, "m", null);
        Delivery d1 = rt.poll(CONSUMER, T, G);
        var n1 = rt.nack(CONSUMER, T, G, d1.getDeliveryId(), "poison message", false);
        assertEquals(TaskQueueRuntime.NackResult.Status.DEAD_LETTER, n1.getStatus());
        assertEquals(com.github.highcumontoa.taskqueueruntimejava.model.DeadLetterRecord.Cause.NON_RETRYABLE,
                n1.getDeadLetter().getCause(), "不可重试消息直接死信，原因可区分");
        log.warn("non-retryable dead letter messageId={} group={} offset={} attempts={} cause={}",
                n1.getDeadLetter().getMessageId(), G, n1.getDeadLetter().getOffset(),
                n1.getAttempts(), n1.getDeadLetter().getCause());
    }

    @Test
    void producerDedupKey_returnsSameMessageAndDistinctFlag() {
        var env = setup(3, Duration.ofMillis(10));
        TaskQueueRuntime rt = env.runtime();
        var p1 = rt.produce(PRODUCER, T, "a", "key-1");
        var p2 = rt.produce(PRODUCER, T, "a", "key-1");
        assertEquals(p1.getMessageId(), p2.getMessageId());
        assertEquals(p1.getOffset(), p2.getOffset());
        assertEquals(false, p1.isDuplicate());
        assertEquals(true, p2.isDuplicate(), "重复生产必须返回既有消息并明确标记 duplicate");
        log.info("producer dedup group={} messageId={} offset={} duplicate1={} duplicate2={}",
                G, p1.getMessageId(), p1.getOffset(), p1.isDuplicate(), p2.isDuplicate());
    }
}
