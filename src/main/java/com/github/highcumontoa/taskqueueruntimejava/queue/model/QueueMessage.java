package com.github.highcumontoa.taskqueueruntimejava.queue.model;

import java.util.Map;

/**
 * 队列消息（仅与主题绑定，不含任何分组消费状态）。
 *
 * @param formatVersion 消息格式版本，用于旧格式识别
 */
public record QueueMessage(String topic,
                           long offset,
                           String messageId,
                           byte[] payload,
                           String contentType,
                           Map<String, String> headers,
                           long enqueueTimeMillis,
                           int formatVersion) {
    public static final int CURRENT_FORMAT_VERSION = 2;
}
