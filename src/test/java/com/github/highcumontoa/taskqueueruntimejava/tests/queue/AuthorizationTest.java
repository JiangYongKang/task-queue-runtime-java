package com.github.highcumontoa.taskqueueruntimejava.tests.queue;

import com.github.highcumontoa.taskqueueruntimejava.queue.config.QueueRuntimeProperties;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.BackpressurePolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueRequest;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Permission;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Principal;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.RetryPolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.TopicSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.QueueRuntimeException;
import com.github.highcumontoa.taskqueueruntimejava.queue.runtime.RuntimeFactory;
import com.github.highcumontoa.taskqueueruntimejava.queue.runtime.TaskQueueRuntime;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 重点：无效凭据、权限不足、越权跨主题访问给出三种可区分错误；
 * 一个主题上的拒绝不得影响其他合法主题。
 */
class AuthorizationTest {

    private static final Logger log = LoggerFactory.getLogger(AuthorizationTest.class);
    private static final String ADMIN = TaskQueueRuntime.DEFAULT_ADMIN_TOKEN;

    private TaskQueueRuntime newRuntime() {
        QueueRuntimeProperties props = QueueRuntimeProperties.defaults();
        return new TaskQueueRuntime(RuntimeFactory.createStorage(props), props);
    }

    @Test
    void invalidMissingAndUnauthorizedCredentialsAreDistinguished() {
        try (TaskQueueRuntime rt = newRuntime()) {
            rt.createTopic(ADMIN, "alpha", new TopicSettings(10, BackpressurePolicy.REJECT));
            rt.createTopic(ADMIN, "beta", new TopicSettings(10, BackpressurePolicy.REJECT));
            rt.createGroup(ADMIN, "alpha", "g", new GroupSettings(60_000L, RetryPolicy.none()));
            rt.createGroup(ADMIN, "beta", "g", new GroupSettings(60_000L, RetryPolicy.none()));

            // 只有 PRODUCE 权限，且仅授权 alpha
            Principal producer = rt.issueCredential(ADMIN, Set.of(Permission.PRODUCE), Set.of("alpha"));
            // 只有 CONSUME 权限，通配主题
            Principal consumer = rt.issueCredential(ADMIN, Set.of(Permission.CONSUME), Set.of("*"));

            // 1) 空凭据
            QueueRuntimeException missing = assertThrows(QueueRuntimeException.class,
                    () -> rt.produce("  ", new EnqueueRequest("alpha", new byte[0], null, null, null)));
            log.info("[AUTH] missing code={}", missing.errorCode());
            assertEquals(ErrorCode.INVALID_CREDENTIAL, missing.errorCode());

            // 2) 无效凭据
            QueueRuntimeException invalid = assertThrows(QueueRuntimeException.class,
                    () -> rt.produce("qt_not_a_real_token",
                            new EnqueueRequest("alpha", new byte[0], null, null, null)));
            log.info("[AUTH] invalid code={}", invalid.errorCode());
            assertEquals(ErrorCode.INVALID_CREDENTIAL, invalid.errorCode());

            // 3) 权限不足：producer 尝试消费；consumer 尝试生产；普通凭据尝试建主题
            QueueRuntimeException noConsume = assertThrows(QueueRuntimeException.class,
                    () -> rt.receive(producer.token(), "alpha", "g", "c", 1, 50L));
            QueueRuntimeException noProduce = assertThrows(QueueRuntimeException.class,
                    () -> rt.produce(consumer.token(),
                            new EnqueueRequest("alpha", new byte[0], null, null, null)));
            QueueRuntimeException noAdmin = assertThrows(QueueRuntimeException.class,
                    () -> rt.createTopic(producer.token(), "gamma", null));
            log.info("[AUTH] permissionDenied codes={},{},{}",
                    noConsume.errorCode(), noProduce.errorCode(), noAdmin.errorCode());
            assertEquals(ErrorCode.PERMISSION_DENIED, noConsume.errorCode());
            assertEquals(ErrorCode.PERMISSION_DENIED, noProduce.errorCode());
            assertEquals(ErrorCode.PERMISSION_DENIED, noAdmin.errorCode());

            // 4) 越权跨主题：producer 只能碰 alpha
            QueueRuntimeException cross = assertThrows(QueueRuntimeException.class,
                    () -> rt.produce(producer.token(),
                            new EnqueueRequest("beta", new byte[0], null, null, null)));
            log.info("[AUTH] crossTopic code={} msg={}", cross.errorCode(), cross.getMessage());
            assertEquals(ErrorCode.CROSS_TOPIC_ACCESS_DENIED, cross.errorCode());

            // 5) 合法操作不受影响：alpha 生产成功、beta 上通配消费者可消费，且彼此隔离
            var receipt = rt.produce(producer.token(),
                    new EnqueueRequest("alpha", "ok".getBytes(), null, null, null));
            log.info("[AUTH] legit produce alpha offset={}", receipt.offset());
            assertEquals(0L, receipt.offset());
            var d = rt.receive(consumer.token(), "alpha", "g", "c", 1, 500L);
            assertEquals(1, d.deliveries().size());
            // 被拒绝的 beta 主题仍可被管理员正常使用
            assertDoesNotThrow(() -> rt.produce(ADMIN,
                    new EnqueueRequest("beta", "fine".getBytes(), null, null, null)));
            log.info("[AUTH] other topics unaffected: beta produce OK, alpha offset still={}",
                    rt.committedOffset(ADMIN, "beta", "g"));
        }
    }
}
