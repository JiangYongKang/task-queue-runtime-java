package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/** 消费者组设置：可见性超时与重试策略。 */
public record GroupSettings(long visibilityTimeoutMillis, RetryPolicy retryPolicy) {
    public static GroupSettings defaults() { return new GroupSettings(30_000L, new RetryPolicy(3, 100L, 2.0, 5_000L)); }
}
