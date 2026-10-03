package com.github.highcumontoa.taskqueueruntimejava.tests;

import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.error.QueueException;
import com.github.highcumontoa.taskqueueruntimejava.model.BackpressureStrategy;
import com.github.highcumontoa.taskqueueruntimejava.model.BatchCommitResult;
import com.github.highcumontoa.taskqueueruntimejava.model.BatchItem;
import com.github.highcumontoa.taskqueueruntimejava.model.BatchProduceResult;
import com.github.highcumontoa.taskqueueruntimejava.model.CommitOutcome;
import com.github.highcumontoa.taskqueueruntimejava.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.model.TopicConfig;
import com.github.highcumontoa.taskqueueruntimejava.runtime.TaskQueueRuntime;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.ADMIN;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.CONSUMER;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.PRODUCER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 批量策略：积压时成批生产（放不下的逐条拒绝、不回退已接收）、
 * 成批确认（部分失败逐条可区分、成功不回退）、批大小上限、
 * 并发批量提交不双重生效、位点连续水位不错乱。
 */
class BatchStrategyTest {

    private static final Logger log = LoggerFactory.getLogger(BatchStrategyTest.class);
    private static final String T = "batch-topic";
    private static final String G = "settle";

    private RuntimeTestSupport.Env env(int capacity, int maxBatch) {
        var env = RuntimeTestSupport.memoryRuntime();
        env.runtime().createTopic(ADMIN, T, new TopicConfig(capacity,
                BackpressureStrategy.BATCH, Duration.ofSeconds(1), Duration.ofSeconds(30),
                com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1,
                Duration.ofMinutes(1), maxBatch));
        env.runtime().createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env, T);
        return env;
    }

    private static List<BatchItem> items(String prefix, int n) {
        List<BatchItem> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(new BatchItem(prefix + "-" + i, null));
        }
        return out;
    }

    @Test
    void produceBatch_partialAcceptanceWhenBacklogged() {
        var env = env(4, 10);
        TaskQueueRuntime rt = env.runtime();
        // 先占 2 个槽位，容量 4：批 4 条只能收下 2 条。
        var first = rt.produceBatch(PRODUCER, T, items("a", 2));
        assertEquals(2, first.getAcceptedCount());

        var second = rt.produceBatch(PRODUCER, T, items("b", 4));
        assertEquals(2, second.getAcceptedCount(), "容量只够再放 2 条");
        assertEquals(2, second.getRejectedCount(), "放不下的逐条拒绝");
        assertEquals(BatchProduceResult.ItemStatus.ACCEPTED, second.getItems().get(0).status());
        assertEquals(2, second.getItems().get(0).offset());
        assertEquals(BatchProduceResult.ItemStatus.ACCEPTED, second.getItems().get(1).status());
        assertEquals(BatchProduceResult.ItemStatus.REJECTED, second.getItems().get(2).status());
        assertEquals(ErrorCode.QUEUE_FULL, second.getItems().get(2).errorCode());
        assertEquals(BatchProduceResult.ItemStatus.REJECTED, second.getItems().get(3).status());
        log.warn("batch partial acceptance accepted={} rejected={} firstRejectedIndex={}",
                second.getAcceptedCount(), second.getRejectedCount(),
                second.getItems().get(2).index());

        // 已接收的不因同批失败回退：4 条都能被消费到。
        for (int i = 0; i < 4; i++) {
            Delivery d = rt.poll(CONSUMER, T, G);
            assertNotNull(d, "已接收消息不得因同批拒绝而丢失 i=" + i);
            rt.commit(CONSUMER, T, G, d.getDeliveryId());
        }
        assertNull(rt.poll(CONSUMER, T, G));

        // 消费释放容量后（时间推进超过保留期并回收），新批次位点继续单调递增。
        env.clock().advance(Duration.ofMinutes(2));
        rt.reclaimTerminated(T);
        var third = rt.produceBatch(PRODUCER, T, items("c", 2));
        assertEquals(2, third.getAcceptedCount());
        assertEquals(4, third.getItems().get(0).offset(), "位点单调不倒退");
        assertEquals(5, third.getItems().get(1).offset());
    }

    @Test
    void produceBatch_oversizeRejected_andDuplicatesDistinguished() {
        var env = env(100, 3);
        TaskQueueRuntime rt = env.runtime();
        // 超过 maxBatchSize 整批拒绝，错误码可区分。
        QueueException tooLarge = assertThrows(QueueException.class,
                () -> rt.produceBatch(PRODUCER, T, items("x", 4)));
        assertEquals(ErrorCode.BATCH_TOO_LARGE, tooLarge.getCode());
        QueueException commitTooLarge = assertThrows(QueueException.class,
                () -> rt.commitBatch(CONSUMER, T, G, List.of("a", "b", "c", "d")));
        assertEquals(ErrorCode.BATCH_TOO_LARGE, commitTooLarge.getCode());
        log.warn("oversize batch rejected code={} maxBatchSize=3", tooLarge.getCode());

        // 批内与跨批的 producerKey 去重：DUPLICATE 可区分且不算失败。
        var r1 = rt.produceBatch(PRODUCER, T, List.of(
                new BatchItem("v1", "k1"), new BatchItem("v1-dup", "k1"),
                new BatchItem("v2", "k2")));
        assertEquals(BatchProduceResult.ItemStatus.ACCEPTED, r1.getItems().get(0).status());
        assertEquals(BatchProduceResult.ItemStatus.DUPLICATE, r1.getItems().get(1).status());
        assertEquals(r1.getItems().get(0).messageId(), r1.getItems().get(1).messageId(),
                "去重命中必须返回既有 messageId");
        assertEquals(BatchProduceResult.ItemStatus.ACCEPTED, r1.getItems().get(2).status());
        assertEquals(3, r1.getAcceptedCount(), "去重命中计入接收，不算失败");
        assertEquals(2, env.backend().readTopic(T).getMessages().size(),
                "去重条目不得重复落库");
    }

    @Test
    void commitBatch_partialFailureDistinguished_andOffsetWatermarkSafe() {
        var env = env(100, 10);
        TaskQueueRuntime rt = env.runtime();
        rt.produceBatch(PRODUCER, T, items("m", 3));
        List<Delivery> ds = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ds.add(rt.poll(CONSUMER, T, G));
        }
        // 乱序 + 混入无效投递：d1, 无效, d0, d2。
        var result = rt.commitBatch(CONSUMER, T, G, List.of(
                ds.get(1).getDeliveryId(), "no-such-delivery",
                ds.get(0).getDeliveryId(), ds.get(2).getDeliveryId()));
        assertEquals(3, result.getCommittedCount());
        assertEquals(1, result.getFailedCount());
        assertEquals(CommitOutcome.COMMITTED, result.getItems().get(0).outcome());
        BatchCommitResult.Item failed = result.getItems().get(1);
        assertNull(failed.outcome());
        assertEquals(ErrorCode.DELIVERY_NOT_FOUND, failed.errorCode(),
                "失败条目必须携带可区分错误码");
        assertEquals("no-such-delivery", failed.deliveryId(),
                "必须能定位是哪一条失败");
        log.warn("batch commit partial failure committed={} failedDelivery={} code={}",
                result.getCommittedCount(), failed.deliveryId(), failed.errorCode());

        // 成功条目不回退：全部终结，位点按连续水位推进到 2。
        assertEquals(2, rt.offsetOf(CONSUMER, T, G).getCommitted());
        assertNull(rt.poll(CONSUMER, T, G), "已提交消息不得再投递");

        // 重复提交同一投递：幂等成功但结论可区分。
        var again = rt.commitBatch(CONSUMER, T, G, List.of(ds.get(0).getDeliveryId()));
        assertEquals(CommitOutcome.ALREADY_COMMITTED, again.getItems().get(0).outcome());
    }

    @Test
    void commitBatch_concurrentBatches_noDoubleEffect() throws Exception {
        var env = env(100, 100);
        TaskQueueRuntime rt = env.runtime();
        int n = 16;
        rt.produceBatch(PRODUCER, T, items("c", n));
        List<String> deliveryIds = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Delivery d = rt.poll(CONSUMER, T, G);
            assertNotNull(d);
            deliveryIds.add(d.getDeliveryId());
        }
        // 两个线程并发提交同一批 deliveryId：每条必须恰好生效一次。
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        ConcurrentLinkedQueue<BatchCommitResult> results = new ConcurrentLinkedQueue<>();
        Runnable task = () -> {
            ready.countDown();
            try {
                go.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            results.add(rt.commitBatch(CONSUMER, T, G, deliveryIds));
        };
        Thread t1 = new Thread(task, "batch-commit-1");
        Thread t2 = new Thread(task, "batch-commit-2");
        t1.start();
        t2.start();
        assertTrue(ready.await(2, TimeUnit.SECONDS));
        go.countDown();
        t1.join(5000);
        t2.join(5000);
        assertEquals(2, results.size());

        int committed = 0;
        int already = 0;
        int failed = 0;
        for (BatchCommitResult r : results) {
            for (BatchCommitResult.Item item : r.getItems()) {
                if (item.outcome() == CommitOutcome.COMMITTED) {
                    committed++;
                } else if (item.outcome() == CommitOutcome.ALREADY_COMMITTED) {
                    already++;
                } else {
                    failed++;
                }
            }
        }
        log.info("concurrent batch commits committed={} alreadyCommitted={} failed={}",
                committed, already, failed);
        assertEquals(n, committed, "每条消息必须恰好首次提交一次，杜绝双重生效");
        assertEquals(n, already, "并发重复提交应幂等返回 ALREADY_COMMITTED");
        assertEquals(0, failed, "并发批量提交不得出现失败条目");
        assertEquals(n - 1, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "并发后位点连续推进到末尾，不错乱");
    }
}
