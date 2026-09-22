package com.github.highcumontoa.taskqueueruntimejava.tests;

import com.github.highcumontoa.taskqueueruntimejava.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.model.TopicConfig;
import com.github.highcumontoa.taskqueueruntimejava.runtime.TaskQueueRuntime;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.ADMIN;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.CONSUMER;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.PRODUCER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 并发消费下“同组一条消息任一时刻仅一个消费者处理”，及关闭时在途消息安全交还。 */
class ConcurrencyLifecycleTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrencyLifecycleTest.class);
    private static final String T = "jobs";
    private static final String G = "workers";

    private RuntimeTestSupport.Env env() {
        var env = RuntimeTestSupport.memoryRuntime();
        env.runtime().createTopic(ADMIN, T, new TopicConfig(1000, null, Duration.ofSeconds(1),
                Duration.ofMinutes(5),
                com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1));
        env.runtime().createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env, T);
        return env;
    }

    @Test
    void concurrentConsumers_eachMessageHandledByOne_atAnyTime() throws Exception {
        var env = env();
        TaskQueueRuntime rt = env.runtime();
        int messageCount = 200;
        int consumerCount = 8;
        for (int i = 0; i < messageCount; i++) {
            rt.produce(PRODUCER, T, "m" + i, null);
        }

        Set<String> inFlight = ConcurrentHashMap.newKeySet();
        Set<String> duplicated = ConcurrentHashMap.newKeySet();
        Set<String> processed = ConcurrentHashMap.newKeySet();
        AtomicInteger doubleOccupancy = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(consumerCount);
        ExecutorService pool = Executors.newFixedThreadPool(consumerCount);

        for (int c = 0; c < consumerCount; c++) {
            final int workerId = c;
            pool.submit(() -> {
                try {
                    while (true) {
                        Delivery d;
                        try {
                            d = rt.poll(CONSUMER, T, G);
                        } catch (RuntimeException e) {
                            continue;
                        }
                        if (d == null) {
                            if (processed.size() >= messageCount) {
                                break;
                            }
                            Thread.sleep(2);
                            continue;
                        }
                        // 互斥断言：同一 messageId 不应同时被两个消费者持有。
                        if (!inFlight.add(d.getMessageId())) {
                            doubleOccupancy.incrementAndGet();
                            duplicated.add(d.getMessageId());
                        }
                        Thread.sleep(1); // 放大竞态窗口
                        rt.commit(CONSUMER, T, G, d.getDeliveryId());
                        inFlight.remove(d.getMessageId());
                        processed.add(d.getMessageId());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        assertTrue(done.await(30, TimeUnit.SECONDS), "消费者必须在限定时间内完成");
        pool.shutdown();

        assertEquals(0, doubleOccupancy.get(),
                "同一条消息任一时刻只能被一个消费者处理，冲突=" + duplicated);
        assertEquals(messageCount, processed.size(), "所有消息必须恰好处理完成");
        log.info("concurrency done topic={} group={} messages={} consumers={} conflicts=0",
                T, G, messageCount, consumerCount);
    }

    @Test
    void shutdown_returnsInflightMessages_forRedelivery() {
        var env = env();
        TaskQueueRuntime rt = env.runtime();
        rt.produce(PRODUCER, T, "inflight-1", null);
        Delivery d = rt.poll(CONSUMER, T, G);
        log.info("leaving inflight on shutdown messageId={} group={} offset={} attempt={}",
                d.getMessageId(), G, d.getOffset(), d.getAttempt());

        // 不提交直接关闭：宽限期为 0，在途消息必须安全交还。
        rt.close(Duration.ZERO);

        // 关闭后拒绝新操作。
        boolean rejected = false;
        try {
            rt.poll(CONSUMER, T, G);
        } catch (RuntimeException e) {
            rejected = true;
        }
        assertTrue(rejected, "关闭后必须拒绝新操作");

        // 用同一后端重建运行时，交还的消息必须可再次投递，不丢失。
        var env2 = RuntimeTestSupport.newRuntime(env.backend());
        RuntimeTestSupport.grantTopic(env2, T);
        Delivery again = env2.runtime().poll(CONSUMER, T, G);
        assertEquals(d.getMessageId(), again.getMessageId(), "关闭时交还的消息必须可重新消费");
        log.info("redelivered after restart messageId={} group={} offset={}",
                again.getMessageId(), G, again.getOffset());
    }
}
