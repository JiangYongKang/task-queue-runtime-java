package com.github.highcumontoa.taskqueueruntimejava.tests;

import com.github.highcumontoa.taskqueueruntimejava.backend.InMemoryQueueBackend;
import com.github.highcumontoa.taskqueueruntimejava.backend.LocalFileQueueBackend;
import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.error.QueueException;
import com.github.highcumontoa.taskqueueruntimejava.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.model.TopicConfig;
import com.github.highcumontoa.taskqueueruntimejava.runtime.TaskQueueRuntime;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.ADMIN;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.CONSUMER;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.PRODUCER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 内存后端与文件后端语义一致；旧/损坏格式必须明确拒绝而非不确定行为。 */
class BackendCompatibilityTest {

    private static final Logger log = LoggerFactory.getLogger(BackendCompatibilityTest.class);
    private static final String T = "compat";
    private static final String G = "g";

    private void scenario(TaskQueueRuntime rt) {
        rt.createTopic(ADMIN, T, new TopicConfig(10, null, Duration.ofSeconds(1),
                Duration.ofSeconds(30),
                com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1));
        rt.createGroup(ADMIN, T, G);
        rt.grant(PRODUCER, T, com.github.highcumontoa.taskqueueruntimejava.model.Permission.PRODUCE);
        rt.grant(CONSUMER, T, com.github.highcumontoa.taskqueueruntimejava.model.Permission.CONSUME);
        var r = rt.produce(PRODUCER, T, "same-semantics", "k1");
        Delivery d = rt.poll(CONSUMER, T, G);
        assertEquals(r.getMessageId(), d.getMessageId());
        var c = rt.commit(CONSUMER, T, G, d.getDeliveryId());
        assertEquals(com.github.highcumontoa.taskqueueruntimejava.model.CommitOutcome.COMMITTED,
                c.getOutcome());
        assertEquals(0, rt.offsetOf(CONSUMER, T, G).getCommitted());
        log.info("semantics ok backend topic={} messageId={} group={} offset={}",
                T, d.getMessageId(), G, d.getOffset());
    }

    @Test
    void memoryAndFileBackends_haveIdenticalSemantics() throws Exception {
        scenario(RuntimeTestSupport.newRuntime(new InMemoryQueueBackend()).runtime());
        Path dir = Files.createTempDirectory("queue-compat-");
        var backend = new LocalFileQueueBackend(dir);
        scenario(RuntimeTestSupport.newRuntime(backend).runtime());
        // 重新打开文件后端后，位点结论一致。
        var reopened = new LocalFileQueueBackend(dir);
        reopened.recover();
        var env2 = RuntimeTestSupport.newRuntime(reopened);
        RuntimeTestSupport.grantTopic(env2, T);
        assertEquals(0, env2.runtime().offsetOf(CONSUMER, T, G).getCommitted(),
                "文件后端恢复后位点语义必须与内存后端一致");
    }

    @Test
    void unsupportedFormatVersion_isRejectedClearly() throws Exception {
        Path dir = Files.createTempDirectory("queue-oldfmt-");
        // 手工写入一个"旧版本"消息文件。
        String oldJson = """
                {
                  "name" : "old",
                  "formatVersion" : 0,
                  "config" : {
                    "capacity" : 10,
                    "backpressureStrategy" : "REJECT",
                    "backpressureTimeout" : "PT1S",
                    "visibilityTimeout" : "PT30S",
                    "backoff" : {
                      "initial" : "PT0.1S", "multiplier" : 2.0,
                      "max" : "PT5S", "maxAttempts" : 3
                    },
                    "formatVersion" : 0
                  },
                  "nextOffset" : 0,
                  "messages" : [ ],
                  "groups" : { },
                  "deadLetters" : [ ],
                  "producerKeyIndex" : { }
                }
                """;
        Files.writeString(dir.resolve("old.topic.json"), oldJson);
        var backend = new LocalFileQueueBackend(dir);
        QueueException ex = assertThrows(QueueException.class, backend::recover);
        assertEquals(ErrorCode.UNSUPPORTED_FORMAT, ex.getCode(),
                "旧版本格式必须以 UNSUPPORTED_FORMAT 明确拒绝");
        log.warn("legacy format rejected code={} reason={}", ex.getCode(), ex.getMessage());
    }

    @Test
    void corruptOrFieldMissingFile_isRejectedClearly() throws Exception {
        Path dir = Files.createTempDirectory("queue-corrupt-");
        Files.writeString(dir.resolve("broken.topic.json"), "{ this is not json");
        var backend = new LocalFileQueueBackend(dir);
        QueueException ex = assertThrows(QueueException.class, backend::recover);
        assertEquals(ErrorCode.UNSUPPORTED_FORMAT, ex.getCode(),
                "损坏数据不得被当成空主题加载，必须明确拒绝");
        log.warn("corrupt file rejected code={} reason={}", ex.getCode(), ex.getMessage());
    }
}
