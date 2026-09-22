package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/** 生产回执。 */
public record EnqueueReceipt(String topic,
                             long offset,
                             String messageId,
                             EnqueueOutcome outcome) {
}
