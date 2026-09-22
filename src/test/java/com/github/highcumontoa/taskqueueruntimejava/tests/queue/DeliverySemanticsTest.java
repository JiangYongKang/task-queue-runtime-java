package com.github.highcumontoa.taskqueueruntimejava.tests.queue;

import com.github.highcumontoa.taskqueueruntimejava.queue.config.QueueRuntimeProperties;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.BackpressurePolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.CommitOutcome;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.CommitReceipt;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.DeliveryReason;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueOutcome;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueReceipt;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueRequest;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.MessageState;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.ReceiveResult;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.RetryPolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.TopicSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.runtime.RuntimeFactory;
import com.github.highcumontoa.taskqueueruntimejava.queue.runtime.TaskQueueRuntime;
import com.github.highcumontoa.taskqueueruntimejava.queue.storage.QueueStorage;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 重点：重复投递、重复提交、重复生产必须与合法重试给出可区分结论，
 * 且业务状态不得重复变更（用计数器模拟业务副作用）。
 */
class DeliverySemanticsTest {

    private static final Logger log = LoggerFactory.getLogger(DeliverySemanticsTest.class);
    private static final String ADMIN = TaskQueueRuntime.DEFAULT_ADMIN_TOKEN;

    private TaskQueueRuntime newRuntime() {
        QueueRuntimeProperties props = QueueRuntimeProperties.defaults();
        QueueStorage storage = RuntimeFactory.createStorage(props);
        return new TaskQueueRuntime(storage, props);
    }

    private void setup(TaskQueueRuntime rt, String topic, String group, int maxDepth,
                       RetryPolicy retryPolicy, long visibilityTimeoutMillis) {
        rt.createTopic(ADMIN, topic, new TopicSettings(maxDepth, BackpressurePolicy.REJECT));
        rt.createGroup(ADMIN, topic, group, new GroupSettings(visibilityTimeoutMillis, retryPolicy));
    }

    /** 同一业务幂等键生产两次：只接受一次，第二次稳定返回 DUPLICATE_PRODUCE 且位点相同。 */
    @Test
    void duplicateProduceIsIdempotentAndDistinguishable() {
        try (TaskQueueRuntime rt = newRuntime()) {
            setup(rt, "orders", "proc", 100, RetryPolicy.none(), 60_000L);
            EnqueueRequest r1 = new EnqueueRequest("orders", "a".getBytes(), null, null, "biz-1");
            EnqueueRequest r2 = new EnqueueRequest("orders", "a".getBytes(), null, null, "biz-1");
            EnqueueReceipt first = rt.produce(ADMIN, r1);
            EnqueueReceipt second = rt.produce(ADMIN, r2);
            log.info("[DUP-PRODUCE] messageId={} offset1={} outcome1={} offset2={} outcome2={}",
                    first.messageId(), first.offset(), first.outcome(), second.offset(), second.outcome());
            assertEquals(EnqueueOutcome.ACCEPTED, first.outcome());
            assertEquals(EnqueueOutcome.DUPLICATE_PRODUCE, second.outcome());
            assertEquals(first.offset(), second.offset());
            assertEquals(first.messageId(), second.messageId());
        }
    }

    /**
     * 可见性超时后同一消息重新投递（合法重试 RETRY），若业务此前已生效（幂等键），
     * 重复提交返回 DUPLICATE_DELIVERY；同一令牌重复提交返回 ALREADY_COMMITTED。两者必须可区分。
     */
    @Test
    void duplicateDeliveryVsDuplicateCommitVsLegitimateRetryAreDistinct() throws Exception {
        List<String> businessEffect = new ArrayList<>(); // 模拟业务副作用
        try (TaskQueueRuntime rt = newRuntime()) {
            setup(rt, "orders", "proc", 100, new RetryPolicy(5, 10, 2.0, 1_000), 120L);
            rt.produce(ADMIN, new EnqueueRequest("orders", "x".getBytes(), null, null, "biz-42"));

            // 第一次投递：业务生效并提交
            ReceiveResult r1 = rt.receive(ADMIN, "orders", "proc", "c1", 1, 200L);
            Delivery d1 = r1.deliveries().get(0);
            assertEquals(DeliveryReason.INITIAL, d1.lease().reason());
            String t1 = d1.lease().deliveryToken();
            businessEffect.add("effect@" + d1.lease().offset());
            CommitReceipt c1 = rt.commit(ADMIN, "orders", "proc", t1, "biz-42");
            log.info("[COMMIT] group={} offset={} messageId={} attempt={} outcome={} committed={}",
                    "proc", c1.offset(), d1.message().messageId(), d1.lease().attempt(),
                    c1.outcome(), c1.committedOffsetAfter());
            assertEquals(CommitOutcome.COMMITTED, c1.outcome());

            // 同一令牌再次提交 -> ALREADY_COMMITTED（幂等，无新副作用）
            CommitReceipt c1again = rt.commit(ADMIN, "orders", "proc", t1, "biz-42");
            log.info("[DUP-COMMIT] group={} offset={} attempt={} outcome={}",
                    "proc", c1again.offset(), d1.lease().attempt(), c1again.outcome());
            assertEquals(CommitOutcome.ALREADY_COMMITTED, c1again.outcome());

            // 制造第二个消息，用极短可见性超时模拟"消费中断"
            rt.produce(ADMIN, new EnqueueRequest("orders", "y".getBytes(), null, null, "biz-43"));
            ReceiveResult r2 = rt.receive(ADMIN, "orders", "proc", "c1", 1, 200L);
            Delivery d2 = r2.deliveries().get(0);
            log.info("[DELIVERY] group={} offset={} messageId={} attempt={} reason={}",
                    "proc", d2.lease().offset(), d2.message().messageId(),
                    d2.lease().attempt(), d2.lease().reason());
            // 模拟消费者崩溃：不提交，等待可见性超时
            Thread.sleep(400L);

            ReceiveResult r3 = rt.receive(ADMIN, "orders", "proc", "c2", 1, 500L);
            Delivery d3 = r3.deliveries().get(0);
            log.info("[REDISPATCH] group={} offset={} messageId={} attempt={} reason={}",
                    "proc", d3.lease().offset(), d3.message().messageId(),
                    d3.lease().attempt(), d3.lease().reason());
            assertEquals(d2.lease().offset(), d3.lease().offset());
            assertEquals(DeliveryReason.REDISPATCH_AFTER_VISIBILITY_TIMEOUT, d3.lease().reason());
            // 旧令牌必须失效，与重复提交区分
            assertThrows(com.github.highcumontoa.taskqueueruntimejava.queue.error.QueueRuntimeException.class,
                    () -> rt.commit(ADMIN, "orders", "proc", d2.lease().deliveryToken(), "biz-43"));

            // 此时业务尚未生效（消费中断），提交应正常 COMMITTED（合法重试后成功）
            businessEffect.add("effect@" + d3.lease().offset());
            CommitReceipt c3 = rt.commit(ADMIN, "orders", "proc", d3.lease().deliveryToken(), "biz-43");
            assertEquals(CommitOutcome.COMMITTED, c3.outcome());
            assertEquals(2L, c3.committedOffsetAfter());
            assertEquals(2, businessEffect.size(), "业务副作用恰好两次（无重复变更）");

            // 若同一业务键在另一位点被再次投递并提交 -> DUPLICATE_DELIVERY。
            // 注意：这里生产侧不携带幂等键（否则会在生产侧被去重），
            // 重复业务键来自消费侧提交参数（模拟不同消息承载了同一业务实体）。
            rt.produce(ADMIN, new EnqueueRequest("orders", "z".getBytes(), null, null, null));
            ReceiveResult r4 = rt.receive(ADMIN, "orders", "proc", "c3", 1, 200L);
            Delivery d4 = r4.deliveries().get(0);
            CommitReceipt c4 = rt.commit(ADMIN, "orders", "proc", d4.lease().deliveryToken(), "biz-42");
            log.info("[DUP-DELIVERY] group={} offset={} attempt={} outcome={}",
                    "proc", c4.offset(), d4.lease().attempt(), c4.outcome());
            assertEquals(CommitOutcome.DUPLICATE_DELIVERY, c4.outcome());
            // 重复投递被压制后位点仍应前进
            assertEquals(3L, c4.committedOffsetAfter());
            assertEquals(2, businessEffect.size(), "重复投递不得产生新副作用");
        }
    }

    /** 同组内任一时刻一条消息只能被一个消费者拿到（租约排他）。 */
    @Test
    void leaseIsExclusiveWithinGroup() {
        try (TaskQueueRuntime rt = newRuntime()) {
            setup(rt, "t", "g", 10, RetryPolicy.none(), 60_000L);
            rt.produce(ADMIN, new EnqueueRequest("t", "m".getBytes(), null, null, null));
            Delivery a = rt.receive(ADMIN, "t", "g", "consumerA", 10, 100L).deliveries().get(0);
            ReceiveResult second = rt.receive(ADMIN, "t", "g", "consumerB", 10, 150L);
            log.info("[EXCLUSIVE] firstConsumer={} offset={} secondPollSize={}",
                    a.lease().consumerId(), a.lease().offset(), second.deliveries().size());
            assertTrue(second.deliveries().isEmpty(), "租约期间第二个消费者不应拿到同一消息");
            assertEquals(MessageState.IN_FLIGHT, rt.messageState(ADMIN, "t", "g", a.lease().offset()));
        }
    }
}
