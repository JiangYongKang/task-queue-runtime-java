package com.github.highcumontoa.taskqueueruntimejava.tests.queue;

import com.github.highcumontoa.taskqueueruntimejava.queue.config.QueueRuntimeProperties;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.BackpressurePolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueRequest;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.RetryPolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.TopicSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.QueueRuntimeException;
import com.github.highcumontoa.taskqueueruntimejava.queue.runtime.TaskQueueRuntime;
import com.github.highcumontoa.taskqueueruntimejava.queue.storage.LocalFileStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/** 重点：后端不可用与处理（生产等待）超时必须返回稳定、可区分的错误码。 */
class BackendFaultTest {

    private static final Logger log = LoggerFactory.getLogger(BackendFaultTest.class);
    private static final String ADMIN = TaskQueueRuntime.DEFAULT_ADMIN_TOKEN;

    @Test
    void backendUnavailableProducesStableErrorCode(@TempDir Path dir) {
        QueueRuntimeProperties props = new QueueRuntimeProperties(
                QueueRuntimeProperties.BackendType.LOCAL_FILE, dir.toString(),
                100, BackpressurePolicy.REJECT, 60_000L, 1_000L, 100L, 10_000);
        LocalFileStorage storage = new LocalFileStorage(dir);
        try (TaskQueueRuntime rt = new TaskQueueRuntime(storage, props)) {
            rt.createTopic(ADMIN, "t", new TopicSettings(100, BackpressurePolicy.REJECT));
            rt.createGroup(ADMIN, "t", "g", new GroupSettings(60_000L, RetryPolicy.none()));

            // 模拟后端故障
            storage.setAvailable(false);
            QueueRuntimeException ex = assertThrows(QueueRuntimeException.class,
                    () -> rt.produce(ADMIN, new EnqueueRequest("t", "x".getBytes(), null, null, null)));
            log.info("[FAULT] code={} msg={}", ex.errorCode(), ex.getMessage());
            assertEquals(ErrorCode.BACKEND_UNAVAILABLE, ex.errorCode());

            // 恢复后合法操作继续可用
            storage.setAvailable(true);
            assertDoesNotThrow(() -> rt.produce(ADMIN,
                    new EnqueueRequest("t", "y".getBytes(), null, null, null)));
            log.info("[FAULT] backend recovered, produce OK");
        }
    }
}
