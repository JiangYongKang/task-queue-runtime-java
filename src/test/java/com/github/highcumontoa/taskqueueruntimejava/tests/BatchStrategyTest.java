package com.github.highcumontoa.taskqueueruntimejava.tests;

import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.error.QueueException;
import com.github.highcumontoa.taskqueueruntimejava.model.BackpressureStrategy;
import com.github.highcumontoa.taskqueueruntimejava.model.BatchProduceResult;
import com.github.highcumontoa.taskqueueruntimejava.model.CommitOutcome;
import com.github.highcumontoa.taskqueueruntimejava.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.model.ProduceItem;
import com.github.highcumontoa.taskqueueruntimejava.model.TopicConfig;
import com.github.highcumontoa.taskqueueruntimejava.runtime.TaskQueueRuntime;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.ADMIN;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.CONSUMER;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.PRODUCER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 批量策略：按可配置批大小成批生产/成批确认；批大小有硬上限；
 * 部分失败逐条可区分、成功项不回退；批量路径同样遵守连续水位线位点规则。
 */
class BatchStrategyTest {

    private static final Logger log = LoggerFactory.getLogger(BatchStrategyTest.class);
    private static final String T = "bulk";
    private static final String G = "settle";

    private RuntimeTestSupport.Env env(int capacity, BackpressureStrategy strategy,
                                       Duration timeout, int batchSize) {
        var env = RuntimeTestSupport.memoryRuntime();
        TopicConfig config = new TopicConfig(capacity, strategy, timeout,
                Duration.ofMinutes(10),
                com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1);
        config.setBatchSize(batchSize);
        env.runtime().createTopic(ADMIN, T, config);
        env.runtime().createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env, T);
        return env;
    }

    private static List<ProduceItem> items(String prefix, int n) {
        List<ProduceItem> list = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            list.add(new ProduceItem(prefix + "-" + i, null));
        }
        return list;
    }

    @Test
    void batchProduce_chunksByConfiguredBatchSize() {
        var env = env(100, BackpressureStrategy.BATCH, Duration.ofSeconds(1), 3);
        TaskQueueRuntime rt = env.runtime();
        var result = rt.produceBatch(PRODUCER, T, items("settle", 7));
        assertEquals(7, result.storedCount());
        assertEquals(0, result.rejectedCount());
        // 位点连续分配，与逐条生产一致。
        for (int i = 0; i < 7; i++) {
            assertEquals(i, result.getItems().get(i).getOffset());
            assertEquals(BatchProduceResult.ItemStatus.STORED, result.getItems().get(i).getStatus());
        }
        // 全部可按序消费。
        for (int i = 0; i < 7; i++) {
            Delivery d = rt.poll(CONSUMER, T, G);
            assertNotNull(d);
            assertEquals(i, d.getOffset());
            rt.commit(CONSUMER, T, G, d.getDeliveryId());
        }
        assertEquals(6, rt.offsetOf(CONSUMER, T, G).getCommitted());
        log.info("batch produce ok stored={} chunks=3+3+1 committed={}",
                result.storedCount(), rt.offsetOf(CONSUMER, T, G).getCommitted());
    }

    @Test
    void batchProduce_partialFailure_keepsSuccessesAndPinpointsFailures() {
        var env = env(4, BackpressureStrategy.REJECT, Duration.ofSeconds(1), 10);
        TaskQueueRuntime rt = env.runtime();
        var result = rt.produceBatch(PRODUCER, T, items("import", 6));
        assertEquals(4, result.storedCount(), "容量内部分必须成功入库");
        assertEquals(2, result.rejectedCount(), "超出容量的部分必须逐条标记失败");
        // 失败项可清楚定位：下标 4、5，错误码 QUEUE_FULL。
        for (int i = 0; i < 6; i++) {
            var item = result.getItems().get(i);
            if (i < 4) {
                assertEquals(BatchProduceResult.ItemStatus.STORED, item.getStatus());
                assertEquals(i, item.getOffset());
            } else {
                assertEquals(BatchProduceResult.ItemStatus.REJECTED, item.getStatus());
                assertEquals(ErrorCode.QUEUE_FULL, item.getErrorCode());
                assertEquals(i, item.getIndex(), "失败项必须能定位到原始下标");
            }
        }
        // 成功项不因同批失败而回退：4 条都可消费。
        for (int i = 0; i < 4; i++) {
            Delivery d = rt.poll(CONSUMER, T, G);
            assertNotNull(d, "成功项不得因同批失败被回退: i=" + i);
            assertEquals("import-" + i, d.getBody());
        }
        assertNull(rt.poll(CONSUMER, T, G));
        log.warn("partial batch stored=4 rejected=2 (QUEUE_FULL), successes intact");
    }

    @Test
    void batchProduce_backpressureTimeout_marksOnlyBlockedItems() {
        var env = env(1, BackpressureStrategy.BATCH, Duration.ofMillis(80), 10);
        TaskQueueRuntime rt = env.runtime();
        rt.produce(PRODUCER, T, "occupant", null);
        var result = rt.produceBatch(PRODUCER, T, items("blocked", 2));
        assertEquals(0, result.storedCount());
        assertEquals(2, result.rejectedCount());
        for (var item : result.getItems()) {
            assertEquals(ErrorCode.BACKPRESSURE_TIMEOUT, item.getErrorCode(),
                    "BATCH 策略下等待超时必须与立即拒绝区分");
        }
        // 占位消息不受影响，仍可正常消费。
        Delivery d = rt.poll(CONSUMER, T, G);
        assertNotNull(d);
        assertEquals("occupant", d.getBody());
        log.warn("batch backpressure timeout per item code={}",
                result.getItems().get(0).getErrorCode());
    }

    @Test
    void batchProduce_requestSizeCap_isEnforced() {
        var env = env(1000, BackpressureStrategy.BATCH, Duration.ofSeconds(1), 10);
        TaskQueueRuntime rt = env.runtime();
        QueueException tooLarge = assertThrows(QueueException.class,
                () -> rt.produceBatch(PRODUCER, T,
                        items("x", TopicConfig.MAX_BATCH_SIZE + 1)));
        assertEquals(ErrorCode.BAD_REQUEST, tooLarge.getCode(), "超过批大小硬上限必须明确拒绝");
        QueueException empty = assertThrows(QueueException.class,
                () -> rt.produceBatch(PRODUCER, T, List.of()));
        assertEquals(ErrorCode.BAD_REQUEST, empty.getCode());
        log.warn("batch size cap enforced max={} code={}", TopicConfig.MAX_BATCH_SIZE,
                tooLarge.getCode());
    }

    @Test
    void batchCommit_partialFailure_watermarkStaysGapSafe() {
        var env = env(100, BackpressureStrategy.BATCH, Duration.ofSeconds(1), 10);
        TaskQueueRuntime rt = env.runtime();
        rt.produceBatch(PRODUCER, T, items("m", 3));
        Delivery d0 = rt.poll(CONSUMER, T, G);
        Delivery d1 = rt.poll(CONSUMER, T, G);
        Delivery d2 = rt.poll(CONSUMER, T, G);

        // 批量确认乱序到达，且混入一个无效投递：部分失败逐条区分。
        var result = rt.commitBatch(CONSUMER, T, G,
                List.of(d2.getDeliveryId(), "no-such-delivery", d0.getDeliveryId()));
        assertEquals(2, result.successCount());
        assertEquals(1, result.failureCount());
        assertEquals(CommitOutcome.COMMITTED, result.getItems().get(0).getOutcome());
        assertNull(result.getItems().get(1).getOutcome());
        assertEquals(ErrorCode.DELIVERY_NOT_FOUND, result.getItems().get(1).getErrorCode(),
                "失败项必须携带可区分错误码");
        assertEquals(CommitOutcome.COMMITTED, result.getItems().get(2).getOutcome());
        // 位点仍守连续水位线：offset=1 未确认，位点只能到 0。
        assertEquals(0, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "批量路径不得因乱序确认跳过在途缺口");

        // 同一批内重复提交：幂等且结论可区分。
        var dup = rt.commitBatch(CONSUMER, T, G,
                List.of(d1.getDeliveryId(), d1.getDeliveryId()));
        assertEquals(CommitOutcome.COMMITTED, dup.getItems().get(0).getOutcome());
        assertEquals(CommitOutcome.ALREADY_COMMITTED, dup.getItems().get(1).getOutcome());
        assertEquals(2, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "缺口补齐后位点越过此前已确认的 offset=2");
        log.info("batch commit partial failure ok, watermark gap-safe committed={}",
                rt.offsetOf(CONSUMER, T, G).getCommitted());
    }

    @Test
    void batchProduce_concurrentWithCommits_noDoubleEffect() throws Exception {
        var env = env(200, BackpressureStrategy.BATCH, Duration.ofSeconds(2), 20);
        TaskQueueRuntime rt = env.runtime();
        int batches = 10;
        int perBatch = 20;
        // 生产者线程：批量导入。
        Thread producer = new Thread(() -> {
            for (int b = 0; b < batches; b++) {
                rt.produceBatch(PRODUCER, T, items("b" + b, perBatch));
            }
        }, "bulk-producer");
        producer.start();
        // 消费者线程：边生产边消费确认。
        int consumed = 0;
        while (consumed < batches * perBatch) {
            Delivery d = rt.poll(CONSUMER, T, G);
            if (d == null) {
                Thread.sleep(2);
                continue;
            }
            var r = rt.commitBatch(CONSUMER, T, G, List.of(d.getDeliveryId()));
            assertEquals(1, r.successCount());
            consumed++;
        }
        producer.join(10_000);
        assertEquals(false, producer.isAlive());
        assertEquals(batches * perBatch - 1, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "并发批量路径位点必须连续推进，无双重生效");
        assertNull(rt.poll(CONSUMER, T, G), "每条消息只能生效一次");
        log.info("concurrent batch produce/commit done consumed={} committed={}",
                consumed, rt.offsetOf(CONSUMER, T, G).getCommitted());
    }
}
