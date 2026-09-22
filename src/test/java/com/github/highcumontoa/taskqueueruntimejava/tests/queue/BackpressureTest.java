package com.github.highcumontoa.taskqueueruntimejava.tests.queue;

import com.github.highcumontoa.taskqueueruntimejava.queue.config.QueueRuntimeProperties;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.BackpressurePolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueOutcome;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueReceipt;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueRequest;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.ReceiveResult;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.RetryPolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.TopicSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.QueueRuntimeException;
import com.github.highcumontoa.taskqueueruntimejava.queue.runtime.RuntimeFactory;
import com.github.highcumontoa.taskqueueruntimejava.queue.runtime.TaskQueueRuntime;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 重点：队列有界；REJECT 立即 QUEUE_FULL；DELAY 等待容量、超时 PRODUCE_TIMEOUT、
 * 消费腾出容量后成功；BATCH 合并/超限拒绝；三种错误稳定可区分。
 */
class BackpressureTest {

    private static final Logger log = LoggerFactory.getLogger(BackpressureTest.class);
    private static final String ADMIN = TaskQueueRuntime.DEFAULT_ADMIN_TOKEN;

    private TaskQueueRuntime newRuntime(BackpressurePolicy policy) {
        QueueRuntimeProperties props = QueueRuntimeProperties.defaults();
        return new TaskQueueRuntime(RuntimeFactory.createStorage(props), props);
    }

    private void fill(TaskQueueRuntime rt, String topic, int n) {
        for (int i = 0; i < n; i++) {
            rt.produce(ADMIN, new EnqueueRequest(topic, ("p" + i).getBytes(), null, null, null));
        }
    }

    @Test
    void rejectPolicyReturnsQueueFullImmediately() {
        try (TaskQueueRuntime rt = newRuntime(BackpressurePolicy.REJECT)) {
            rt.createTopic(ADMIN, "bounded", new TopicSettings(3, BackpressurePolicy.REJECT));
            fill(rt, "bounded", 3);
            QueueRuntimeException ex = assertThrows(QueueRuntimeException.class,
                    () -> rt.produce(ADMIN,
                            new EnqueueRequest("bounded", "overflow".getBytes(), null, null, null)));
            log.info("[BP] full code={} msg={}", ex.errorCode(), ex.getMessage());
            assertEquals(ErrorCode.QUEUE_FULL, ex.errorCode());
            assertEquals(3, rt.depth(ADMIN, "bounded"));
        }
    }

    @Test
    void delayPolicyWaitsForCapacityAndTimesOutDistinctly() throws Exception {
        try (TaskQueueRuntime rt = newRuntime(BackpressurePolicy.DELAY)) {
            rt.createTopic(ADMIN, "bp-delay", new TopicSettings(2, BackpressurePolicy.DELAY));
            rt.createGroup(ADMIN, "bp-delay", "g", new GroupSettings(60_000L, RetryPolicy.none()));
            fill(rt, "bp-delay", 2);

            // 容量满：等待 200ms 无人消费 -> PRODUCE_TIMEOUT
            long t0 = System.currentTimeMillis();
            QueueRuntimeException timeout = assertThrows(QueueRuntimeException.class,
                    () -> rt.produce(ADMIN,
                            new EnqueueRequest("bp-delay", "x".getBytes(), null, null, null), 200L));
            log.info("[BP] delay-wait code={} waitedMs={}", timeout.errorCode(),
                    System.currentTimeMillis() - t0);
            assertEquals(ErrorCode.PRODUCE_TIMEOUT, timeout.errorCode());

            // 另一线程在 ~150ms 后消费并提交，腾出容量
            Thread consumer = new Thread(() -> {
                try {
                    Thread.sleep(150L);
                    ReceiveResult rr = rt.receive(ADMIN, "bp-delay", "g", "c", 1, 500L);
                    Delivery d = rr.deliveries().get(0);
                    rt.commit(ADMIN, "bp-delay", "g", d.lease().deliveryToken(), null);
                    log.info("[BP] consumed to free capacity offset={}", d.lease().offset());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            consumer.start();
            EnqueueReceipt receipt = rt.produce(ADMIN,
                    new EnqueueRequest("bp-delay", "y".getBytes(), null, null, null), 2_000L);
            consumer.join();
            log.info("[BP] delayed produce succeeded offset={} outcome={}",
                    receipt.offset(), receipt.outcome());
            assertEquals(EnqueueOutcome.ACCEPTED, receipt.outcome());
        }
    }

    @Test
    void batchPolicyMergesPayloadsAndRejectsOversize() {
        try (TaskQueueRuntime rt = newRuntime(BackpressurePolicy.BATCH)) {
            rt.createTopic(ADMIN, "bp-batch", new TopicSettings(100, BackpressurePolicy.BATCH));
            EnqueueReceipt receipt = rt.produceBatch(ADMIN, "bp-batch",
                    List.of("a".getBytes(), "b".getBytes(), "c".getBytes()), 10, 1_000L);
            log.info("[BP] batch accepted offset={} outcome={}", receipt.offset(), receipt.outcome());
            assertEquals(EnqueueOutcome.BATCHED, receipt.outcome());

            QueueRuntimeException tooBig = assertThrows(QueueRuntimeException.class,
                    () -> rt.produceBatch(ADMIN, "bp-batch",
                            List.of("a".getBytes(), "b".getBytes()), 1, 1_000L));
            log.info("[BP] batch oversize code={} msg={}", tooBig.errorCode(), tooBig.getMessage());
            assertEquals(ErrorCode.BATCH_REJECTED, tooBig.errorCode());

            // BATCH 主题上使用单条 produce 且队列满时，错误码仍稳定为 QUEUE_FULL 并提示使用批量入口
            rt.createTopic(ADMIN, "bp-batch2", new TopicSettings(1, BackpressurePolicy.BATCH));
            fill(rt, "bp-batch2", 1);
            QueueRuntimeException full = assertThrows(QueueRuntimeException.class,
                    () -> rt.produce(ADMIN,
                            new EnqueueRequest("bp-batch2", "z".getBytes(), null, null, null)));
            assertEquals(ErrorCode.QUEUE_FULL, full.errorCode());
        }
    }
}
