package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/** 死信记录：消息进入死信后的可查询快照。 */
public record DeadLetterRecord(String topic,
                               String group,
                               long offset,
                               String messageId,
                               int attempts,
                               DeadLetterReason reason,
                               String errorCode,
                               String errorMessage,
                               long deadAtMillis) {
}
