package com.github.highcumontoa.taskqueueruntimejava.web;

import com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig;
import com.github.highcumontoa.taskqueueruntimejava.model.BackpressureStrategy;
import com.github.highcumontoa.taskqueueruntimejava.model.CommitResult;
import com.github.highcumontoa.taskqueueruntimejava.model.DeadLetterRecord;
import com.github.highcumontoa.taskqueueruntimejava.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.model.OffsetInfo;
import com.github.highcumontoa.taskqueueruntimejava.model.TopicConfig;
import com.github.highcumontoa.taskqueueruntimejava.runtime.TaskQueueRuntime;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 队列 REST 端点。所有端点通过 {@code X-Queue-Token} 头传递凭据，
 * 授权由运行时统一执行；错误由 {@link ApiExceptionHandler} 统一映射。
 *
 * <pre>
 * POST /api/queue/topics/{topic}/groups/{group}          创建组
 * POST /api/queue/topics/{topic}/messages                生产
 * POST /api/queue/topics/{topic}/messages:batch          批量生产（部分失败逐条结论）
 * POST /api/queue/topics/{topic}/groups/{group}/poll     拉取
 * POST /api/queue/topics/{topic}/groups/{group}/commit   提交
 * POST /api/queue/topics/{topic}/groups/{group}/commits:batch  批量确认
 * POST /api/queue/topics/{topic}/groups/{group}/nack     失败确认
 * GET  /api/queue/topics/{topic}/groups/{group}/offset   位点（含保留边界）
 * POST /api/queue/topics/{topic}/groups/{group}/replay   位点重放
 * GET  /api/queue/topics/{topic}/dead-letters            死信查询
 * </pre>
 */
@RestController
@RequestMapping("/api/queue")
public class QueueController {

    private final TaskQueueRuntime runtime;

    public QueueController(TaskQueueRuntime runtime) {
        this.runtime = runtime;
    }

    public record CreateTopicRequest(Integer capacity, String backpressureStrategy,
                                    Long backpressureTimeoutMillis,
                                    Long visibilityTimeoutMillis,
                                    Long initialBackoffMillis, Double multiplier,
                                    Long maxBackoffMillis, Integer maxAttempts,
                                    Integer batchSize) {
    }

    public record ProduceRequest(String body, String producerKey) {
    }

    public record BatchProduceRequest(List<com.github.highcumontoa.taskqueueruntimejava.model.ProduceItem> items) {
    }

    public record BatchCommitRequest(List<String> deliveryIds) {
    }

    public record NackRequest(String deliveryId, String error, Boolean retryable) {
    }

    @PostMapping("/topics/{topic}")
    public Map<String, Object> createTopic(
            @RequestHeader(value = "X-Queue-Token", required = false) String token,
            @PathVariable String topic,
            @RequestBody(required = false) CreateTopicRequest req) {
        TopicConfig config = buildConfig(req);
        runtime.createTopic(token, topic, config);
        return Map.of("topic", topic, "created", true);
    }

    private TopicConfig buildConfig(CreateTopicRequest req) {
        if (req == null) {
            return TopicConfig.defaults();
        }
        BackoffConfig defaults = BackoffConfig.defaults();
        BackoffConfig backoff = new BackoffConfig(
                Duration.ofMillis(req.initialBackoffMillis() == null
                        ? defaults.getInitial().toMillis() : req.initialBackoffMillis()),
                req.multiplier() == null ? defaults.getMultiplier() : req.multiplier(),
                Duration.ofMillis(req.maxBackoffMillis() == null
                        ? defaults.getMax().toMillis() : req.maxBackoffMillis()),
                req.maxAttempts() == null ? defaults.getMaxAttempts() : req.maxAttempts());
        TopicConfig config = new TopicConfig(
                req.capacity() == null ? 1000 : req.capacity(),
                req.backpressureStrategy() == null
                        ? BackpressureStrategy.REJECT
                        : BackpressureStrategy.valueOf(req.backpressureStrategy()),
                Duration.ofMillis(req.backpressureTimeoutMillis() == null
                        ? 2000 : req.backpressureTimeoutMillis()),
                Duration.ofMillis(req.visibilityTimeoutMillis() == null
                        ? 30_000 : req.visibilityTimeoutMillis()),
                backoff, 1);
        if (req.batchSize() != null) {
            config.setBatchSize(req.batchSize());
        }
        return config;
    }

    @PostMapping("/topics/{topic}/groups/{group}")
    public Map<String, Object> createGroup(
            @RequestHeader(value = "X-Queue-Token", required = false) String token,
            @PathVariable String topic, @PathVariable String group) {
        runtime.createGroup(token, topic, group);
        return Map.of("topic", topic, "group", group, "created", true);
    }

    @PostMapping("/topics/{topic}/messages")
    public TaskQueueRuntime.ProduceReceipt produce(
            @RequestHeader(value = "X-Queue-Token", required = false) String token,
            @PathVariable String topic, @RequestBody ProduceRequest req) {
        return runtime.produce(token, topic, req.body(), req.producerKey());
    }

    @PostMapping("/topics/{topic}/messages:batch")
    public com.github.highcumontoa.taskqueueruntimejava.model.BatchProduceResult produceBatch(
            @RequestHeader(value = "X-Queue-Token", required = false) String token,
            @PathVariable String topic, @RequestBody BatchProduceRequest req) {
        return runtime.produceBatch(token, topic, req.items());
    }

    @PostMapping("/topics/{topic}/groups/{group}/commits:batch")
    public com.github.highcumontoa.taskqueueruntimejava.model.BatchCommitResult commitBatch(
            @RequestHeader(value = "X-Queue-Token", required = false) String token,
            @PathVariable String topic, @PathVariable String group,
            @RequestBody BatchCommitRequest req) {
        return runtime.commitBatch(token, topic, group, req.deliveryIds());
    }

    @PostMapping("/topics/{topic}/groups/{group}/poll")
    public Delivery poll(
            @RequestHeader(value = "X-Queue-Token", required = false) String token,
            @PathVariable String topic, @PathVariable String group) {
        return runtime.poll(token, topic, group);
    }

    @PostMapping("/topics/{topic}/groups/{group}/commit")
    public CommitResult commit(
            @RequestHeader(value = "X-Queue-Token", required = false) String token,
            @PathVariable String topic, @PathVariable String group,
            @RequestBody NackRequest req) {
        return runtime.commit(token, topic, group, req.deliveryId());
    }

    @PostMapping("/topics/{topic}/groups/{group}/nack")
    public TaskQueueRuntime.NackResult nack(
            @RequestHeader(value = "X-Queue-Token", required = false) String token,
            @PathVariable String topic, @PathVariable String group,
            @RequestBody NackRequest req) {
        return runtime.nack(token, topic, group, req.deliveryId(),
                req.error() == null ? "" : req.error(),
                req.retryable() == null || req.retryable());
    }

    @GetMapping("/topics/{topic}/groups/{group}/offset")
    public OffsetInfo offset(
            @RequestHeader(value = "X-Queue-Token", required = false) String token,
            @PathVariable String topic, @PathVariable String group) {
        return runtime.offsetOf(token, topic, group);
    }

    @PostMapping("/topics/{topic}/groups/{group}/replay")
    public OffsetInfo replay(
            @RequestHeader(value = "X-Queue-Token", required = false) String token,
            @PathVariable String topic, @PathVariable String group,
            @RequestParam long targetOffset) {
        return runtime.replay(token, topic, group, targetOffset);
    }

    @GetMapping("/topics/{topic}/dead-letters")
    public List<DeadLetterRecord> deadLetters(
            @RequestHeader(value = "X-Queue-Token", required = false) String token,
            @PathVariable String topic) {
        return runtime.deadLetters(token, topic);
    }
}
