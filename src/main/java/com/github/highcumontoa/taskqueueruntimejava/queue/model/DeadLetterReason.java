package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/** 进入死信的原因。 */
public enum DeadLetterReason {
    RETRY_EXHAUSTED,
    NON_RETRYABLE
}
