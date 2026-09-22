package com.github.highcumontoa.taskqueueruntimejava.tests.queue;

import com.github.highcumontoa.taskqueueruntimejava.queue.config.QueueRuntimeProperties;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.BackpressurePolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.CommitOutcome;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueRequest;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.NackOutcome;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.ReceiveResult;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.RetryPolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.TopicSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.QueueRuntimeException;
import com.github.highcumontoa.taskqueueruntimejava.queue.runtime.TaskQueueRuntime;
import com.github.highcumontoa.taskqueueruntimejava.queue.storage.AbstractInMemoryStorage;
import com.github.highcumontoa.taskqueueruntimejava.queue.storage.LocalFileStorage;
import com.github.highcumontoa.taskqueueruntimejava.queue.storage.QueueStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 重点：内存后端与本地可恢复后端在相同操作序列下结果一致；
 * 旧版本（v1）消息格式可识别并迁移；高于当前版本的格式被明确拒绝。
 */
class BackendCompatibilityTest {

    private static final Logger log = LoggerFactory.getLogger(BackendCompatibilityTest.class);
    private static final String ADMIN = TaskQueueRuntime.DEFAULT_ADMIN_TOKEN;

    /** 在给定存储上执行固定操作序列，返回可比较的结果轨迹。 */
    private List<String> scenario(QueueStorage storage, QueueRuntimeProperties props) {
        try (TaskQueueRuntime rt = new TaskQueueRuntime(storage, props)) {
            java.util.List<String> trace = new java.util.ArrayList<>();
            rt.createTopic(ADMIN, "t", new TopicSettings(100, BackpressurePolicy.REJECT));
            rt.createGroup(ADMIN, "t", "g",
                    new GroupSettings(60_000L, new RetryPolicy(2, 10, 2.0, 1_000)));
            rt.produce(ADMIN, new EnqueueRequest("t", "m0".getBytes(), null, null, "k0"));
            rt.produce(ADMIN, new EnqueueRequest("t", "m1".getBytes(), null, null, "k1"));

            Delivery d0 = rt.receive(ADMIN, "t", "g", "c", 1, 500L).deliveries().get(0);
            trace.add("deliver:" + d0.lease().offset() + "#" + d0.lease().attempt()
                    + "#" + d0.lease().reason());
            trace.add("commit:" + rt.commit(ADMIN, "t", "g", d0.lease().deliveryToken(), "k0").outcome());

            Delivery d1 = rt.receive(ADMIN, "t", "g", "c", 1, 500L).deliveries().get(0);
            trace.add("deliver:" + d1.lease().offset() + "#" + d1.lease().attempt());
            trace.add("nack:" + rt.nack(ADMIN, "t", "g", d1.lease().deliveryToken(),
                    "E", "boom", true).outcome());
            Delivery d1b = rt.receive(ADMIN, "t", "g", "c", 1, 2_000L).deliveries().get(0);
            trace.add("redeliver:" + d1b.lease().offset() + "#" + d1b.lease().attempt()
                    + "#" + d1b.lease().reason());
            trace.add("dead:" + rt.nack(ADMIN, "t", "g", d1b.lease().deliveryToken(),
                    "E", "boom", true).deadLetterRecord().reason());
            trace.add("committedOffset:" + rt.committedOffset(ADMIN, "t", "g"));
            trace.add("dlqSize:" + rt.deadLetters(ADMIN, "t", "g").size());
            return trace;
        }
    }

    @Test
    void memoryAndLocalFileBackendsHaveIdenticalSemantics(@TempDir Path dir) {
        QueueRuntimeProperties memProps = QueueRuntimeProperties.defaults();
        List<String> memoryTrace = scenario(new AbstractInMemoryStorage(), memProps);

        QueueRuntimeProperties fileProps = new QueueRuntimeProperties(
                QueueRuntimeProperties.BackendType.LOCAL_FILE, dir.toString(),
                100, BackpressurePolicy.REJECT, 60_000L, 1_000L, 50L, 10_000);
        List<String> fileTrace = scenario(new LocalFileStorage(dir), fileProps);

        log.info("[BACKEND] memory={}", memoryTrace);
        log.info("[BACKEND] file  ={}", fileTrace);
        assertEquals(memoryTrace, fileTrace, "内存与本地后端语义轨迹必须一致");
        assertTrue(memoryTrace.contains("committedOffset:2"));
    }

    @Test
    void legacyV1MessageFormatIsMigratedOnRecovery(@TempDir Path dir) throws IOException {
        // 手工布置一个 v1（无 contentType/headers/formatVersion 字段）消息日志与主题元数据
        Path topicDir = dir.resolve("topics").resolve("legacy");
        Files.createDirectories(topicDir.resolve("groups"));
        String v1Line = "{\"topic\":\"legacy\",\"offset\":0,\"messageId\":\"old-msg\","
                + "\"payload\":\"" + java.util.Base64.getEncoder()
                .encodeToString("legacy-payload".getBytes(StandardCharsets.UTF_8))
                + "\",\"enqueueTimeMillis\":1700000000000}";
        Files.writeString(topicDir.resolve("messages.log"), v1Line + System.lineSeparator());
        Files.writeString(topicDir.resolve("meta.json"),
                "{\"version\":2,\"maxDepth\":100,\"backpressurePolicy\":\"REJECT\","
                        + "\"nextOffset\":1,\"logStartOffset\":0}");

        QueueRuntimeProperties props = new QueueRuntimeProperties(
                QueueRuntimeProperties.BackendType.LOCAL_FILE, dir.toString(),
                100, BackpressurePolicy.REJECT, 60_000L, 1_000L, 50L, 10_000);
        LocalFileStorage storage = new LocalFileStorage(dir); // 恢复时迁移 v1
        try (TaskQueueRuntime rt = new TaskQueueRuntime(storage, props)) {
            rt.createGroup(ADMIN, "legacy", "g", new GroupSettings(60_000L, RetryPolicy.none()));
            Delivery d = rt.receive(ADMIN, "legacy", "g", "c", 1, 500L).deliveries().get(0);
            log.info("[LEGACY] recovered messageId={} offset={} contentType={} payload={}",
                    d.message().messageId(), d.lease().offset(),
                    d.message().contentType(), new String(d.message().payload(), StandardCharsets.UTF_8));
            assertEquals("old-msg", d.message().messageId());
            assertEquals("application/octet-stream", d.message().contentType(),
                    "v1 缺失 contentType 必须补确定默认值");
            assertEquals("legacy-payload", new String(d.message().payload(), StandardCharsets.UTF_8));
            assertEquals(2, d.message().formatVersion(), "迁移后为当前格式版本");
            var commit = rt.commit(ADMIN, "legacy", "g", d.lease().deliveryToken(), null);
            assertEquals(CommitOutcome.COMMITTED, commit.outcome());
        }
    }

    @Test
    void unknownFutureFormatIsRejectedClearly(@TempDir Path dir) throws IOException {
        Path topicDir = dir.resolve("topics").resolve("future");
        Files.createDirectories(topicDir.resolve("groups"));
        String futureLine = "{\"formatVersion\":99,\"topic\":\"future\",\"offset\":0,"
                + "\"messageId\":\"future-msg\",\"payload\":\"\",\"enqueueTimeMillis\":1}";
        Files.writeString(topicDir.resolve("messages.log"), futureLine + System.lineSeparator());
        Files.writeString(topicDir.resolve("meta.json"),
                "{\"version\":2,\"maxDepth\":100,\"backpressurePolicy\":\"REJECT\","
                        + "\"nextOffset\":1,\"logStartOffset\":0}");

        QueueRuntimeProperties props = new QueueRuntimeProperties(
                QueueRuntimeProperties.BackendType.LOCAL_FILE, dir.toString(),
                100, BackpressurePolicy.REJECT, 60_000L, 1_000L, 50L, 10_000);
        QueueRuntimeException ex = assertThrows(QueueRuntimeException.class,
                () -> new LocalFileStorage(dir));
        log.info("[FORMAT] future-version code={} msg={}",
                ex instanceof com.github.highcumontoa.taskqueueruntimejava.queue.error.MessageFormatException
                        ? ex.errorCode() : "n/a", ex.getMessage());
        assertEquals(ErrorCode.UNKNOWN_MESSAGE_FORMAT, ex.errorCode());
    }
}
