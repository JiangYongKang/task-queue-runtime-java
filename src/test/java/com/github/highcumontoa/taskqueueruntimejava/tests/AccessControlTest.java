package com.github.highcumontoa.taskqueueruntimejava.tests;

import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.error.QueueException;
import com.github.highcumontoa.taskqueueruntimejava.model.Permission;
import com.github.highcumontoa.taskqueueruntimejava.model.TopicConfig;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.ADMIN;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.CONSUMER;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.OUTSIDER;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.PRODUCER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 无效凭据、跨主题访问、权限不足三类拒绝必须原因可区分且互不影响。 */
class AccessControlTest {

    private static final Logger log = LoggerFactory.getLogger(AccessControlTest.class);
    private static final String T1 = "topic-a";
    private static final String T2 = "topic-b";

    private RuntimeTestSupport.Env env() {
        var env = RuntimeTestSupport.memoryRuntime();
        env.runtime().createTopic(ADMIN, T1,
                new TopicConfig(10, null, Duration.ofSeconds(1), Duration.ofSeconds(30),
                        com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1));
        env.runtime().createTopic(ADMIN, T2,
                new TopicConfig(10, null, Duration.ofSeconds(1), Duration.ofSeconds(30),
                        com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1));
        env.runtime().createGroup(ADMIN, T1, "g");
        return env;
    }

    private static QueueException expect(Runnable r) {
        return assertThrows(QueueException.class, r::run);
    }

    @Test
    void invalidCredential_crossTopic_andMissingPermission_areDistinguishable() {
        var env = env();
        var rt = env.runtime();
        env.runtime().grant(PRODUCER, T1, Permission.PRODUCE);
        env.runtime().grant(CONSUMER, T1, Permission.CONSUME);

        QueueException noToken = expect(() -> rt.produce(null, T1, "x", null));
        QueueException badToken = expect(() -> rt.produce("no-such-token", T1, "x", null));
        assertEquals(ErrorCode.INVALID_CREDENTIAL, noToken.getCode());
        assertEquals(ErrorCode.INVALID_CREDENTIAL, badToken.getCode(), "无效凭据必须独立可区分");
        log.warn("invalid credentials rejected code={} reason1={} reason2={}",
                badToken.getCode(), noToken.getMessage(), badToken.getMessage());

        // outsider 对 T1 没有任何主题级授权 -> CROSS_TOPIC_ACCESS
        QueueException cross = expect(() -> rt.produce(OUTSIDER, T1, "x", null));
        assertEquals(ErrorCode.CROSS_TOPIC_ACCESS, cross.getCode(),
                "未授权访问该主题必须与权限不足区分");
        log.warn("cross-topic rejected principal={} topic={} code={}", OUTSIDER, T1, cross.getCode());

        // consumer 对 T1 有 CONSUME 但无 PRODUCE -> PERMISSION_DENIED
        QueueException denied = expect(() -> rt.produce(CONSUMER, T1, "x", null));
        assertEquals(ErrorCode.PERMISSION_DENIED, denied.getCode(),
                "同主题权限不足必须与跨主题访问区分");
        log.warn("permission denied topic={} required=PRODUCE code={}", T1, denied.getCode());

        // consumer 对 T2 完全无授权 -> CROSS_TOPIC_ACCESS，不能被 T1 的授权带过
        QueueException cross2 = expect(() -> rt.poll(CONSUMER, T2, "g"));
        assertEquals(ErrorCode.CROSS_TOPIC_ACCESS, cross2.getCode());
    }

    @Test
    void rejectingOneTopic_mustNotAffectOtherAuthorizedTopics() {
        var env = env();
        var rt = env.runtime();
        env.runtime().grant(PRODUCER, T1, Permission.PRODUCE);
        env.runtime().grant(PRODUCER, T2, Permission.PRODUCE);

        // 对 T1 的越权拒绝
        QueueException ex = expect(() -> rt.produce(OUTSIDER, T1, "x", null));
        assertEquals(ErrorCode.CROSS_TOPIC_ACCESS, ex.getCode());
        // T2 的合法生产必须照常成功
        var receipt = rt.produce(PRODUCER, T2, "ok", null);
        assertEquals(0, receipt.getOffset());
        log.info("other topic unaffected topic={} messageId={} offset={}",
                T2, receipt.getMessageId(), receipt.getOffset());
    }

    @Test
    void nonAdminCannotCreateTopics() {
        var env = env();
        var rt = env.runtime();
        QueueException ex = expect(() -> rt.createTopic(PRODUCER, "topic-c", TopicConfig.defaults()));
        assertEquals(ErrorCode.CROSS_TOPIC_ACCESS, ex.getCode(),
                "无该主题任何授权的管理员操作按跨主题拒绝");
    }
}
