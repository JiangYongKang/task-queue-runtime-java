package com.github.highcumontoa.taskqueueruntimejava.queue.web;

import java.util.List;

/** REST 层请求/响应 DTO（record 集合）。 */
public final class WebDtos {

    private WebDtos() {
    }

    public record TopicRequest(Integer maxDepth, String backpressurePolicy) { }

    public record GroupRequest(Long visibilityTimeoutMillis,
                               Integer maxAttempts,
                               Long baseDelayMillis,
                               Double multiplier,
                               Long maxDelayMillis) { }

    public record ProduceRequest(String payloadBase64,
                                 String payload,
                                 String contentType,
                                 java.util.Map<String, String> headers,
                                 String idempotencyKey,
                                 Long waitMillis) { }

    public record BatchProduceRequest(List<String> payloadsBase64, Integer maxBatch, Long waitMillis) { }

    public record ReceiveRequest(String consumerId, Integer maxMessages, Long timeoutMillis) { }

    public record CommitRequest(String deliveryToken, String idempotencyKey) { }

    public record NackRequest(String deliveryToken, String errorCode, String errorMessage,
                              Boolean retryable) { }

    public record ResetOffsetRequest(Long target) { }

    public record CredentialRequest(List<String> permissions, List<String> grantedTopics) { }

    public record ErrorBody(String errorCode, String message) { }

    public record OffsetResponse(long committedOffset) { }

    public record CredentialResponse(String token, List<String> permissions, List<String> grantedTopics) { }
}
