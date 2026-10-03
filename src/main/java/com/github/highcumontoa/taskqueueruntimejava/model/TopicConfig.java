package com.github.highcumontoa.taskqueueruntimejava.model;

import java.time.Duration;

/**
 * 主题配置：容量上限、背压策略与等待时长、
 * 可见性超时、退避/重试配置、存储格式版本、
 * 终结消息保留期（retentionMillis）与批量大小上限（maxBatchSize）。
 */
public final class TopicConfig {
    /** 批量大小硬上限，超出该值的配置会被钳制到此上限。 */
    public static final int MAX_BATCH_SIZE_HARD_LIMIT = 1000;

    private int capacity;
    private BackpressureStrategy backpressureStrategy;
    private Duration backpressureTimeout;
    private Duration visibilityTimeout;
    private BackoffConfig backoff;
    private int formatVersion;
    /** 终结（全组提交/死信）消息的保留期；到期后可被回收。 */
    private Duration retentionMillis;
    /** 单批生产/提交的最大条数。 */
    private int maxBatchSize;

    public TopicConfig() {
    }

    public TopicConfig(int capacity,
                       BackpressureStrategy backpressureStrategy,
                       Duration backpressureTimeout,
                       Duration visibilityTimeout,
                       BackoffConfig backoff,
                       int formatVersion) {
        this(capacity, backpressureStrategy, backpressureTimeout, visibilityTimeout,
                backoff, formatVersion, Duration.ofMinutes(1), 200);
    }

    public TopicConfig(int capacity,
                       BackpressureStrategy backpressureStrategy,
                       Duration backpressureTimeout,
                       Duration visibilityTimeout,
                       BackoffConfig backoff,
                       int formatVersion,
                       Duration retentionMillis,
                       int maxBatchSize) {
        this.capacity = capacity;
        this.backpressureStrategy = backpressureStrategy;
        this.backpressureTimeout = backpressureTimeout;
        this.visibilityTimeout = visibilityTimeout;
        this.backoff = backoff;
        this.formatVersion = formatVersion;
        this.retentionMillis = retentionMillis;
        this.maxBatchSize = maxBatchSize;
    }

    public int getCapacity() { return capacity; }
    public void setCapacity(int capacity) { this.capacity = capacity; }
    public BackpressureStrategy getBackpressureStrategy() { return backpressureStrategy; }
    public void setBackpressureStrategy(BackpressureStrategy backpressureStrategy) { this.backpressureStrategy = backpressureStrategy; }
    public Duration getBackpressureTimeout() { return backpressureTimeout; }
    public void setBackpressureTimeout(Duration backpressureTimeout) { this.backpressureTimeout = backpressureTimeout; }
    public Duration getVisibilityTimeout() { return visibilityTimeout; }
    public void setVisibilityTimeout(Duration visibilityTimeout) { this.visibilityTimeout = visibilityTimeout; }
    public BackoffConfig getBackoff() { return backoff; }
    public void setBackoff(BackoffConfig backoff) { this.backoff = backoff; }
    public int getFormatVersion() { return formatVersion; }
    public void setFormatVersion(int formatVersion) { this.formatVersion = formatVersion; }
    public Duration getRetentionMillis() { return retentionMillis; }
    public void setRetentionMillis(Duration retentionMillis) { this.retentionMillis = retentionMillis; }
    public int getMaxBatchSize() { return maxBatchSize; }
    public void setMaxBatchSize(int maxBatchSize) { this.maxBatchSize = maxBatchSize; }

    public static TopicConfig defaults() {
        return new TopicConfig(1000, BackpressureStrategy.REJECT, Duration.ofSeconds(2),
                Duration.ofSeconds(30), BackoffConfig.defaults(), 1);
    }
}
