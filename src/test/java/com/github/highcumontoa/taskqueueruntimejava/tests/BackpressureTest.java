package com.github.highcumontoa.taskqueueruntimejava.tests;

import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.error.QueueException;
import com.github.highcumontoa.taskqueueruntimejava.model.BackpressureStrategy;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 有界队列：REJECT 立即拒绝、WAIT 超时拒绝、消费后恢复生产；错误码稳定可区分。 */
class BackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(BackpressureTest.class);
    private static final String T = "bounded";
    private static final String G = "g";

    private RuntimeTestSupport.Env env(int capacity, BackpressureStrategy strategy, Duration timeout) {
        var env = RuntimeTestSupport.memoryRuntime();
        env.runtime().createTopic(ADMIN, T, new TopicConfig(capacity, strategy, timeout,
                Duration.ofSeconds(30),
                com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1));
        env.runtime().createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env, T);
        return env;
    }

    @Test
    void rejectStrategy_returnsStableQueueFull() {
        var env = env(2, BackpressureStrategy.REJECT, Duration.ofSeconds(1));
        var rt = env.runtime();
        rt.produce(PRODUCER, T, "m1", null);
        rt.produce(PRODUCER, T, "m2", null);
        QueueException ex = assertThrows(QueueException.class,
                () -> rt.produce(PRODUCER, T, "m3", null));
        assertEquals(ErrorCode.QUEUE_FULL, ex.getCode(), "达到容量上限必须稳定返回 QUEUE_FULL");
        log.warn("queue full topic={} capacity=2 code={} reason={}", T, ex.getCode(), ex.getMessage());

        // 消费并提交释放容量后，生产恢复，不允许内存无界占用。
        var d = rt.poll(CONSUMER, T, G);
        rt.commit(CONSUMER, T, G, d.getDeliveryId());
        var receipt = rt.produce(PRODUCER, T, "m3", null);
        assertEquals(2, receipt.getOffset());
        log.info("capacity freed, produce recovered topic={} messageId={} offset={}",
                T, receipt.getMessageId(), receipt.getOffset());
    }

    @Test
    void waitStrategy_timesOutWithDistinctError() {
        var env = env(1, BackpressureStrategy.WAIT, Duration.ofMillis(120));
        var rt = env.runtime();
        rt.produce(PRODUCER, T, "only", null);

        long start = System.currentTimeMillis();
        QueueException ex = assertThrows(QueueException.class,
                () -> rt.produce(PRODUCER, T, "blocked", null));
        long elapsed = System.currentTimeMillis() - start;
        assertEquals(ErrorCode.BACKPRESSURE_TIMEOUT, ex.getCode(),
                "等待超时必须与立即满队错误区分");
        assertTrue(elapsed >= 100, "必须经历等待窗口而非立即返回, elapsed=" + elapsed);
        log.warn("backpressure timeout topic={} elapsedMs={} code={}", T, elapsed, ex.getCode());
    }

    @Test
    void waitStrategy_unblocksWhenConsumerCommits() throws Exception {
        var env = env(1, BackpressureStrategy.WAIT, Duration.ofSeconds(2));
        var rt = env.runtime();
        rt.produce(PRODUCER, T, "first", null);

        Thread producer = new Thread(() -> {
            try {
                var r = rt.produce(PRODUCER, T, "second", null);
                log.info("unblocked produce messageId={} offset={}", r.getMessageId(), r.getOffset());
            } catch (RuntimeException e) {
                log.error("unexpected produce failure", e);
            }
        }, "blocked-producer");
        producer.start();

        Thread.sleep(100);
        var d = rt.poll(CONSUMER, T, G);
        rt.commit(CONSUMER, T, G, d.getDeliveryId());
        producer.join(3000);
        assertEquals(false, producer.isAlive(), "消费者提交后等待中的生产者必须被唤醒");
        log.info("waiter released after commit topic={} group={} offset={}", T, G, d.getOffset());
    }
}
