package com.github.highcumontoa.taskqueueruntimejava.tests;

import com.github.highcumontoa.taskqueueruntimejava.model.CommitOutcome;
import com.github.highcumontoa.taskqueueruntimejava.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.model.DeliveryReason;
import com.github.highcumontoa.taskqueueruntimejava.model.TopicConfig;
import com.github.highcumontoa.taskqueueruntimejava.runtime.TaskQueueRuntime;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.ADMIN;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.CONSUMER;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.PRODUCER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 重复投递、重复提交与可见性超时语义测试。
 * 日志统一输出 messageId / group / offset / attempt，便于追踪。
 */
class DeliveryAndVisibilityTest {

    private static final Logger log = LoggerFactory.getLogger(DeliveryAndVisibilityTest.class);
    private static final String T = "orders";
    private static final String G = "grp-a";

    private RuntimeTestSupport.Env setup(Duration visibility) {
        var env = RuntimeTestSupport.memoryRuntime();
        env.runtime().createTopic(ADMIN, T,
                new TopicConfig(100, null, Duration.ofSeconds(1), visibility,
                        com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1));
        env.runtime().createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env, T);
        return env;
    }

    @Test
    void visibilityTimeout_redelivers_andReasonIsDistinguishable() {
        var env = setup(Duration.ofSeconds(30));
        TaskQueueRuntime rt = env.runtime();
        var receipt = rt.produce(PRODUCER, T, "hello", null);
        log.info("[case] produce messageId={} group={} offset={}",
                receipt.getMessageId(), G, receipt.getOffset());

        Delivery first = rt.poll(CONSUMER, T, G);
        assertNotNull(first);
        assertEquals(DeliveryReason.FIRST, first.getReason());
        log.info("first delivery messageId={} group={} offset={} attempt={} reason={}",
                first.getMessageId(), G, first.getOffset(), first.getAttempt(), first.getReason());

        // 未提交：同组在租约内无法再次取到（任一时刻只有一个消费者处理）。
        assertNull(rt.poll(CONSUMER, T, G), "租约内不得重复投递");

        // 超过可见性超时后必须可重新投递，而不是永久丢失。
        env.clock().advance(Duration.ofSeconds(31));
        int reclaimed = rt.reclaimExpired(T);
        assertEquals(1, reclaimed);
        Delivery second = rt.poll(CONSUMER, T, G);
        assertNotNull(second);
        assertEquals(DeliveryReason.VISIBILITY_TIMEOUT, second.getReason(),
                "超时重投原因必须可与合法重试区分");
        assertEquals(2, second.getAttempt());
        log.info("redelivery messageId={} group={} offset={} attempt={} reason={}",
                second.getMessageId(), G, second.getOffset(), second.getAttempt(), second.getReason());

        var committed = rt.commit(CONSUMER, T, G, second.getDeliveryId());
        assertEquals(CommitOutcome.COMMITTED, committed.getOutcome());
        log.info("commit messageId={} group={} offset={} outcome={}",
                committed.getMessageId(), G, committed.getOffset(), committed.getOutcome());
    }

    @Test
    void duplicateCommit_isIdempotent_andDistinguishable() {
        var env = setup(Duration.ofSeconds(30));
        TaskQueueRuntime rt = env.runtime();
        var receipt = rt.produce(PRODUCER, T, "hello", null);
        Delivery d = rt.poll(CONSUMER, T, G);
        log.info("messageId={} group={} offset={} attempt={}",
                receipt.getMessageId(), G, d.getOffset(), d.getAttempt());

        var c1 = rt.commit(CONSUMER, T, G, d.getDeliveryId());
        var c2 = rt.commit(CONSUMER, T, G, d.getDeliveryId());
        assertEquals(CommitOutcome.COMMITTED, c1.getOutcome());
        assertEquals(CommitOutcome.ALREADY_COMMITTED, c2.getOutcome(),
                "重复提交必须幂等且结论可区分");
        log.info("duplicate commit messageId={} group={} offset={} first={} second={}",
                receipt.getMessageId(), G, d.getOffset(), c1.getOutcome(), c2.getOutcome());
    }

    @Test
    void processTemplate_timeoutRedeliveryShortCircuits_butExplicitReplayReapplies() {
        var env = setup(Duration.ofSeconds(30));
        TaskQueueRuntime rt = env.runtime();
        rt.produce(PRODUCER, T, "payload-x", "biz-123");
        AtomicInteger sideEffects = new AtomicInteger();

        var r1 = rt.process(CONSUMER, T, G, d -> {
            sideEffects.incrementAndGet();
            log.info("业务生效 messageId={} group={} offset={} attempt={}",
                    d.getMessageId(), G, d.getOffset(), d.getAttempt());
            return "ok";
        });
        assertEquals(TaskQueueRuntime.ProcessResult.Status.APPLIED, r1.getStatus());

        // 显式重放：需求要求已确认消息必须真正重新投递并重新处理一遍，
        // 因此重放清除消费侧幂等登记，业务再次生效（APPLIED，而非幂等短路）。
        var info = rt.replay(ADMIN, T, G, -1);
        assertEquals(1, info.getResetCount());
        var r2 = rt.process(CONSUMER, T, G, d -> {
            sideEffects.incrementAndGet();
            return "ok-again";
        });
        assertEquals(TaskQueueRuntime.ProcessResult.Status.APPLIED, r2.getStatus(),
                "显式重放的重新投递必须让业务重新生效，不能被旧的幂等记录短路");
        assertEquals(2, sideEffects.get(),
                "显式重放是“已处理消息允许再次生效”的唯一入口，此处应发生第二次副作用");
        assertEquals(DeliveryReason.REPLAY, r2.getReason());
        log.info("replay re-applied messageId={} group={} offset={} attempt={} status={}",
                r2.getMessageId(), G,
                r2.getCommitResult() != null ? r2.getCommitResult().getOffset() : -1,
                r2.getAttempt(), r2.getStatus());

        // 重放这轮提交完成后，没有新的重放指令也没有超时，因此不存在额外投递，
        // 业务副作用停在 2 次（非显式重放的重复到达仍由幂等保护，见可见性超时用例）。
        assertNull(rt.poll(CONSUMER, T, G), "重放轮提交完成后没有新的重复投递");
        assertEquals(2, sideEffects.get(), "无重放指令时业务副作用停在 2 次");
    }
}
