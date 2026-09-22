package com.github.highcumontoa.taskqueueruntimejava.queue.model;

import java.util.Map;

/** 生产请求。idempotencyKey 为业务去重键，可空。 */
public record EnqueueRequest(String topic,
                             byte[] payload,
                             String contentType,
                             Map<String, String> headers,
                             String idempotencyKey) {
}
