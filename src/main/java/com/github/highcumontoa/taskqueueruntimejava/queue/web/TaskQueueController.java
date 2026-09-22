package com.github.highcumontoa.taskqueueruntimejava.queue.web;

import com.github.highcumontoa.taskqueueruntimejava.queue.model.BackpressurePolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueReceipt;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueRequest;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Permission;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Principal;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.ReceiveResult;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.RetryPolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.TopicSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.runtime.TaskQueueRuntime;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 运行时的薄 HTTP 封装。所有接口通过 {@code X-Queue-Token} 头携带凭据。
 * 错误以稳定 errorCode + HTTP 状态返回，见 {@link QueueExceptionAdvice}。
 */
@RestController
@RequestMapping("/api/queue")
public class TaskQueueController {

    private final TaskQueueRuntime runtime;

    public TaskQueueController(TaskQueueRuntime runtime) {
        this.runtime = runtime;
    }

    @PostMapping("/topics/{topic}")
    public Map<String, Object> createTopic(@RequestHeader("X-Queue-Token") String token,
                                           @PathVariable String topic,
                                           @RequestBody(required = false) WebDtos.TopicRequest body) {
        TopicSettings settings = null;
        if (body != null && (body.maxDepth() != null || body.backpressurePolicy() != null)) {
            settings = new TopicSettings(
                    body.maxDepth() != null ? body.maxDepth() : 10_000,
                    body.backpressurePolicy() != null
                            ? BackpressurePolicy.valueOf(body.backpressurePolicy())
                            : BackpressurePolicy.REJECT);
        }
        runtime.createTopic(token, topic, settings);
        return Map.of("topic", topic, "status", "created");
    }

    @PostMapping("/topics/{topic}/groups/{group}")
    public Map<String, Object> createGroup(@RequestHeader("X-Queue-Token") String token,
                                           @PathVariable String topic,
                                           @PathVariable String group,
                                           @RequestBody(required = false) WebDtos.GroupRequest body) {
        GroupSettings settings = null;
        if (body != null && (body.visibilityTimeoutMillis() != null || body.maxAttempts() != null)) {
            RetryPolicy defaults = GroupSettings.defaults().retryPolicy();
            RetryPolicy policy = new RetryPolicy(
                    body.maxAttempts() != null ? body.maxAttempts() : defaults.maxAttempts(),
                    body.baseDelayMillis() != null ? body.baseDelayMillis() : defaults.baseDelayMillis(),
                    body.multiplier() != null ? body.multiplier() : defaults.multiplier(),
                    body.maxDelayMillis() != null ? body.maxDelayMillis() : defaults.maxDelayMillis());
            settings = new GroupSettings(
                    body.visibilityTimeoutMillis() != null
                            ? body.visibilityTimeoutMillis()
                            : GroupSettings.defaults().visibilityTimeoutMillis(),
                    policy);
        }
        runtime.createGroup(token, topic, group, settings);
        return Map.of("topic", topic, "group", group, "status", "created");
    }

    @PostMapping("/topics/{topic}/messages")
    public EnqueueReceipt produce(@RequestHeader("X-Queue-Token") String token,
                                  @PathVariable String topic,
                                  @RequestBody WebDtos.ProduceRequest body) {
        byte[] payload = decodePayload(body.payloadBase64(), body.payload());
        EnqueueRequest request = new EnqueueRequest(topic, payload, body.contentType(),
                body.headers(), body.idempotencyKey());
        long wait = body.waitMillis() != null ? body.waitMillis() : -1L;
        return wait >= 0 ? runtime.produce(token, request, wait) : runtime.produce(token, request);
    }

    @PostMapping("/topics/{topic}/messages/batch")
    public EnqueueReceipt produceBatch(@RequestHeader("X-Queue-Token") String token,
                                       @PathVariable String topic,
                                       @RequestBody WebDtos.BatchProduceRequest body) {
        List<byte[]> payloads = body.payloadsBase64() == null ? List.of()
                : body.payloadsBase64().stream().map(b -> Base64.getDecoder().decode(b)).toList();
        int maxBatch = body.maxBatch() != null ? body.maxBatch() : 100;
        long wait = body.waitMillis() != null ? body.waitMillis() : -1L;
        return runtime.produceBatch(token, topic, payloads, maxBatch, wait < 0 ? 1_000L : wait);
    }

    @PostMapping("/topics/{topic}/groups/{group}/receive")
    public ReceiveResult receive(@RequestHeader("X-Queue-Token") String token,
                                 @PathVariable String topic,
                                 @PathVariable String group,
                                 @RequestBody(required = false) WebDtos.ReceiveRequest body) {
        String consumerId = body != null && body.consumerId() != null ? body.consumerId() : "default-consumer";
        int max = body != null && body.maxMessages() != null ? body.maxMessages() : 1;
        long timeout = body != null && body.timeoutMillis() != null ? body.timeoutMillis() : 0L;
        return runtime.receive(token, topic, group, consumerId, max, timeout);
    }

    @PostMapping("/topics/{topic}/groups/{group}/commit")
    public Map<String, Object> commit(@RequestHeader("X-Queue-Token") String token,
                                      @PathVariable String topic,
                                      @PathVariable String group,
                                      @RequestBody WebDtos.CommitRequest body) {
        var receipt = runtime.commit(token, topic, group, body.deliveryToken(), body.idempotencyKey());
        return Map.of("topic", topic, "group", group, "offset", receipt.offset(),
                "outcome", receipt.outcome().name(),
                "committedOffsetAfter", receipt.committedOffsetAfter());
    }

    @PostMapping("/topics/{topic}/groups/{group}/nack")
    public Map<String, Object> nack(@RequestHeader("X-Queue-Token") String token,
                                    @PathVariable String topic,
                                    @PathVariable String group,
                                    @RequestBody WebDtos.NackRequest body) {
        var receipt = runtime.nack(token, topic, group, body.deliveryToken(),
                body.errorCode(), body.errorMessage(),
                body.retryable() != null && body.retryable());
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("topic", topic);
        out.put("group", group);
        out.put("offset", receipt.offset());
        out.put("outcome", receipt.outcome().name());
        if (receipt.nextAttempt() != null) {
            out.put("nextAttempt", receipt.nextAttempt());
        }
        if (receipt.nextVisibleAtMillis() != null) {
            out.put("nextVisibleAtMillis", receipt.nextVisibleAtMillis());
        }
        if (receipt.deadLetterRecord() != null) {
            out.put("deadLetter", receipt.deadLetterRecord());
        }
        return out;
    }

    @GetMapping("/topics/{topic}/groups/{group}/offset")
    public WebDtos.OffsetResponse offset(@RequestHeader("X-Queue-Token") String token,
                                         @PathVariable String topic,
                                         @PathVariable String group) {
        return new WebDtos.OffsetResponse(runtime.committedOffset(token, topic, group));
    }

    @PostMapping("/topics/{topic}/groups/{group}/reset-offset")
    public Map<String, Object> resetOffset(@RequestHeader("X-Queue-Token") String token,
                                           @PathVariable String topic,
                                           @PathVariable String group,
                                           @RequestBody WebDtos.ResetOffsetRequest body) {
        long target = runtime.resetOffset(token, topic, group, body.target());
        return Map.of("topic", topic, "group", group, "committedOffset", target);
    }

    @GetMapping("/topics/{topic}/groups/{group}/dead-letters")
    public Object deadLetters(@RequestHeader("X-Queue-Token") String token,
                              @PathVariable String topic,
                              @PathVariable String group) {
        return runtime.deadLetters(token, topic, group);
    }

    @PostMapping("/credentials")
    public WebDtos.CredentialResponse issueCredential(@RequestHeader("X-Queue-Token") String token,
                                                      @RequestBody WebDtos.CredentialRequest body) {
        Set<Permission> perms = body.permissions() == null ? Set.of()
                : body.permissions().stream().map(Permission::valueOf).collect(Collectors.toSet());
        Set<String> topics = body.grantedTopics() == null ? Set.of() : Set.copyOf(body.grantedTopics());
        Principal principal = runtime.issueCredential(token, perms, topics);
        return new WebDtos.CredentialResponse(principal.token(),
                principal.permissions().stream().map(Enum::name).toList(),
                List.copyOf(principal.grantedTopics()));
    }

    private static byte[] decodePayload(String base64, String plain) {
        if (base64 != null) {
            return Base64.getDecoder().decode(base64);
        }
        return plain == null ? new byte[0] : plain.getBytes(StandardCharsets.UTF_8);
    }
}
