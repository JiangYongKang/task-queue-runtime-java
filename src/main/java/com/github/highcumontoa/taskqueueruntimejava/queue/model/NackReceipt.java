package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/** nack 回执。 */
public record NackReceipt(String topic,
                          String group,
                          long offset,
                          String deliveryToken,
                          NackOutcome outcome,
                          Integer nextAttempt,
                          Long nextVisibleAtMillis,
                          DeadLetterRecord deadLetterRecord) {
}
