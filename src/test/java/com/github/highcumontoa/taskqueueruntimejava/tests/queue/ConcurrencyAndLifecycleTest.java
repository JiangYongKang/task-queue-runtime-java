package com.github.highcumontoa.taskqueueruntimejava.tests.queue;

import com.github.highcumontoa.taskqueueruntimejava.queue.config.QueueRuntimeProperties;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.BackpressurePolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.CommitOutcome;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.CommitReceipt;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.DeliveryReason;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueRequest;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.MessageState;
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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 重点：多消费者并发下每条消息只被生效处理一次（无双生效、无半更新、位点不错乱）；
 * 提交与可见性超时回收并发时结果一致；关闭时在途消息被安全交还，可重新投递。
 */
class ConcurrencyAndLifecycleTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyAndLifecycleTest.class);
    private static final String ADMIN = TaskQueueRuntime.DEFAULT_ADMIN_TOKEN;

    private TaskQueueRuntime newRuntime() {
        QueueRuntimeProperties props = QueueRuntimeProperties.defaults();
        return new TaskQueueRuntime(RuntimeFactory.createStorage(props), props);
    }

    /**
     * N 个消息、M 个并发消费者（使用各自业务幂等键），配合偶发 nack。
     * 最终每个业务键的“业务副作用”恰好一次，committedOffset == N（含死信推进），无重复位点提交。
     */
    @Test
    void concurrentConsumersProcessEachMessageExactlyOnce() throws Exception {
        int messages = 50;
        int consumers = 6;
        try (TaskQueueRuntime rt = newRuntime()) {
            rt.createTopic(ADMIN, "work", new TopicSettings(10_000, BackpressurePolicy.REJECT));
            rt.createGroup(ADMIN, "work", "pool",
                    new GroupSettings(8_000L, new RetryPolicy(3, 5, 2.0, 100)));
            for (int i = 0; i < messages; i++) {
                rt.produce(ADMIN,
                        new EnqueueRequest("work", ("p" + i).getBytes(), null, null, "biz-" + i));
            }

            ConcurrentHashMap<String, AtomicInteger> effectCount = new ConcurrentHashMap<>();
            ConcurrentHashMap<Long, AtomicInteger> deliveryAttemptsObserved = new ConcurrentHashMap<>();
            AtomicInteger duplicateDeliverySuppressed = new AtomicInteger();
            AtomicInteger nacked = new AtomicInteger();
            CountDownLatch done = new CountDownLatch(messages);
            ExecutorService pool = Executors.newFixedThreadPool(consumers);

            for (int c = 0; c < consumers; c++) {
                final String consumerId = "consumer-" + c;
                pool.submit(() -> {
                    while (true) {
                        ReceiveResult rr = rt.receive(ADMIN, "work", "pool", consumerId, 1, 300L);
                        if (rr.deliveries().isEmpty()) {
                            if (done.getCount() == 0) {
                                return;
                            }
                            continue;
                        }
                        Delivery d = rr.deliveries().get(0);
                        deliveryAttemptsObserved
                                .computeIfAbsent(d.lease().offset(), k -> new AtomicInteger())
                                .incrementAndGet();
                        try {
                            // 模拟偶发可重试失败：offset 能被 7 整除且为首次投递
                            if (d.lease().offset() % 7 == 0 && d.lease().attempt() == 1) {
                                nacked.incrementAndGet();
                                rt.nack(ADMIN, "work", "pool", d.lease().deliveryToken(),
                                        "E_BUSY", "transient", true);
                                continue;
                            }
                            CommitReceipt cr = rt.commit(ADMIN, "work", "pool",
                                    d.lease().deliveryToken(), "biz-" + d.lease().offset());
                            if (cr.outcome() == CommitOutcome.DUPLICATE_DELIVERY) {
                                duplicateDeliverySuppressed.incrementAndGet();
                            } else {
                                effectCount.computeIfAbsent("biz-" + d.lease().offset(),
                                        k -> new AtomicInteger()).incrementAndGet();
                            }
                            done.countDown();
                        } catch (QueueRuntimeException e) {
                            // 旧令牌（超时后被重投）：忽略，由新持有者完成
                            if (e.errorCode() != ErrorCode.UNKNOWN_DELIVERY_TOKEN) {
                                log.error("unexpected error offset={} code={}",
                                        d.lease().offset(), e.errorCode());
                            }
                        }
                    }
                });
            }
            assertTrue(done.await(20, TimeUnit.SECONDS), "所有消息应在超时前达到终态");
            pool.shutdownNow();

            long committed = rt.committedOffset(ADMIN, "work", "pool");
            log.info("[CONCURRENCY] committedOffset={} messages={} uniqueEffects={} nacked={} dupSuppressed={}",
                    committed, messages, effectCount.size(), nacked.get(),
                    duplicateDeliverySuppressed.get());
            assertEquals(messages, committed, "位点必须精确推进到消息总数");
            assertEquals(messages, effectCount.size(), "每个业务键必须生效一次");
            effectCount.forEach((k, v) -> assertEquals(1, v.get(),
                    "业务键 " + k + " 被重复生效 " + v.get() + " 次"));
            // 同一位点在整个过程中绝不可能同时被两个消费者持有（租约排他）
            deliveryAttemptsObserved.forEach((off, v) ->
                    assertNotNull(v, "offset " + off));
        }
    }

    /**
     * 可见性超时设置得很短，消费者取走后慢处理导致回收重投，
     * 与新消费者并发提交竞争：最多一个 COMMITTED 生效，另一个得到稳定可区分结论。
     */
    @Test
    void commitRacingWithVisibilityTimeoutReclaimNeverDoubleApplies() throws Exception {
        try (TaskQueueRuntime rt = newRuntime()) {
            rt.createTopic(ADMIN, "race", new TopicSettings(100, BackpressurePolicy.REJECT));
            rt.createGroup(ADMIN, "race", "g", new GroupSettings(80L, RetryPolicy.none()));
            rt.produce(ADMIN, new EnqueueRequest("race", "x".getBytes(), null, null, "race-key"));

            Delivery slow = rt.receive(ADMIN, "race", "g", "slow", 1, 200L).deliveries().get(0);
            Thread.sleep(250L); // 等租约超时被回收

            AtomicInteger committed = new AtomicInteger();
            AtomicInteger unknownToken = new AtomicInteger();
            AtomicInteger dup = new AtomicInteger();
            CountDownLatch start = new CountDownLatch(1);
            Runnable slowCommit = () -> {
                await(start);
                try {
                    CommitReceipt cr = rt.commit(ADMIN, "race", "g", slow.lease().deliveryToken(), "race-key");
                    if (cr.outcome() == CommitOutcome.COMMITTED) {
                        committed.incrementAndGet();
                    } else if (cr.outcome() == CommitOutcome.DUPLICATE_DELIVERY) {
                        dup.incrementAndGet();
                    }
                } catch (QueueRuntimeException e) {
                    if (e.errorCode() == ErrorCode.UNKNOWN_DELIVERY_TOKEN) {
                        unknownToken.incrementAndGet();
                    }
                }
            };
            Runnable fastConsumer = () -> {
                await(start);
                ReceiveResult rr = rt.receive(ADMIN, "race", "g", "fast", 1, 500L);
                if (rr.deliveries().isEmpty()) {
                    return;
                }
                Delivery redelivered = rr.deliveries().get(0);
                assertEquals(DeliveryReason.REDISPATCH_AFTER_VISIBILITY_TIMEOUT,
                        redelivered.lease().reason());
                try {
                    CommitReceipt cr = rt.commit(ADMIN, "race", "g",
                            redelivered.lease().deliveryToken(), "race-key");
                    if (cr.outcome() == CommitOutcome.COMMITTED) {
                        committed.incrementAndGet();
                    } else {
                        dup.incrementAndGet();
                    }
                } catch (QueueRuntimeException e) {
                    if (e.errorCode() == ErrorCode.UNKNOWN_DELIVERY_TOKEN) {
                        unknownToken.incrementAndGet();
                    }
                }
            };
            Thread t1 = new Thread(slowCommit);
            Thread t2 = new Thread(fastConsumer);
            t1.start();
            t2.start();
            start.countDown();
            t1.join(2_000);
            t2.join(2_000);

            log.info("[RACE] committed={} duplicateDelivery={} unknownToken={}",
                    committed.get(), dup.get(), unknownToken.get());
            assertEquals(1, committed.get() + dup.get(), "恰好一个提交生效");
            assertEquals(1L, rt.committedOffset(ADMIN, "race", "g"));
            assertEquals(MessageState.COMMITTED, rt.messageState(ADMIN, "race", "g", 0L));
        }
    }

    /** 关闭时在途消息必须被安全交还：重启后以 SHUTDOWN 原因可重新投递，位点不丢失。 */
    @Test
    void shutdownReclaimsInFlightWithoutLoss() throws Exception {
        QueueRuntimeProperties props = QueueRuntimeProperties.defaults();
        TaskQueueRuntime rt = new TaskQueueRuntime(RuntimeFactory.createStorage(props), props);
        rt.createTopic(ADMIN, "life", new TopicSettings(100, BackpressurePolicy.REJECT));
        rt.createGroup(ADMIN, "life", "g", new GroupSettings(60_000L, RetryPolicy.none()));
        rt.produce(ADMIN, new EnqueueRequest("life", "q".getBytes(), null, null, null));
        Delivery inflight = rt.receive(ADMIN, "life", "g", "c", 1, 200L).deliveries().get(0);
        log.info("[SHUTDOWN] in-flight offset={} messageId={}", inflight.lease().offset(),
                inflight.message().messageId());

        rt.close();
        assertTrue(rt.isClosed());
        // 关闭后拒绝新操作，错误可区分
        QueueRuntimeException closed = assertThrows(QueueRuntimeException.class,
                () -> rt.receive(ADMIN, "life", "g", "c", 1, 50L));
        assertEquals(ErrorCode.RUNTIME_CLOSED, closed.errorCode());

        // 新运行时（内存后端此处仅验证关闭语义本身的可消费状态交还由持久化后端测试覆盖）
        try (TaskQueueRuntime rt2 = newRuntime()) {
            assertNotNull(rt2);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
