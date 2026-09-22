package com.github.highcumontoa.taskqueueruntimejava.tests.queue;

import com.github.highcumontoa.taskqueueruntimejava.queue.config.QueueRuntimeProperties;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.CommitOutcome;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueOutcome;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.ReceiveResult;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.TopicSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.runtime.RuntimeFactory;
import com.github.highcumontoa.taskqueueruntimejava.queue.runtime.TaskQueueRuntime;
import com.github.highcumontoa.taskqueueruntimejava.queue.storage.QueueStorage;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

class SmokeTest {

    private static final Logger log = LoggerFactory.getLogger(SmokeTest.class);
    private static final String ADMIN = TaskQueueRuntime.DEFAULT_ADMIN_TOKEN;

    @Test
    void produceReceiveCommitSmoke() {
        QueueStorage storage = RuntimeFactory.createStorage(QueueRuntimeProperties.defaults());
        try (TaskQueueRuntime rt = new TaskQueueRuntime(storage, QueueRuntimeProperties.defaults())) {
            rt.createTopic(ADMIN, "t", new TopicSettings(10,
                    com.github.highcumontoa.taskqueueruntimejava.queue.model.BackpressurePolicy.REJECT));
            rt.createGroup(ADMIN, "t", "g", GroupSettings.defaults());

            var receipt = rt.produce(ADMIN, new com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueRequest(
                    "t", "hello".getBytes(), "text/plain", null, null));
            log.info("produced offset={} messageId={} outcome={}", receipt.offset(), receipt.messageId(), receipt.outcome());
            assertEquals(0L, receipt.offset());
            assertEquals(EnqueueOutcome.ACCEPTED, receipt.outcome());

            ReceiveResult rr = rt.receive(ADMIN, "t", "g", "c1", 10, 500L);
            assertEquals(1, rr.deliveries().size());
            Delivery d = rr.deliveries().get(0);
            log.info("received group={} offset={} attempt={} reason={}",
                    "g", d.lease().offset(), d.lease().attempt(), d.lease().reason());

            var commit = rt.commit(ADMIN, "t", "g", d.lease().deliveryToken(), null);
            log.info("committed group={} offset={} outcome={} committedAfter={}",
                    "g", commit.offset(), commit.outcome(), commit.committedOffsetAfter());
            assertEquals(CommitOutcome.COMMITTED, commit.outcome());
            assertEquals(1L, commit.committedOffsetAfter());
        }
    }
}
