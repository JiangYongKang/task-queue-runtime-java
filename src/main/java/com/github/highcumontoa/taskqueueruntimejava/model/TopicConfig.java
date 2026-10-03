package com.github.highcumontoa.taskqueueruntimejava.model;

import java.time.Duration;

/**
 * 主题配置：容量上限、背压策略与等待时长、
 * 可见性超时、退避/重试配置、存储格式版本。
 */
public final class TopicConfig {
    /** 批大小硬上限：配置与单次批量请求都不允许超过该值。 */
    public static final int MAX_BATCH_SIZE = 500;
    /** 批量接口默认批大小。 */
    public static final int DEFAULT_BATCH_SIZE = 100;

    private int capacity;
    private BackpressureStrategy backpressureStrategy;
    private Duration backpressureTimeout;
    private Duration visibilityTimeout;
    private BackoffConfig backoff;
    private int formatVersion;
    private int batchSize = DEFAULT_BATCH_SIZE;

    public TopicConfig() {
    }

    public TopicConfig(int capacity,
                       BackpressureStrategy backpressureStrategy,
                       Duration backpressureTimeout,
                       Duration visibilityTimeout,
                       BackoffConfig backoff,
                       int formatVersion) {
        this.capacity = capacity;
        this.backpressureStrategy = backpressureStrategy;
        this.backpressureTimeout = backpressureTimeout;
        this.visibilityTimeout = visibilityTimeout;
        this.backoff = backoff;
        this.formatVersion = formatVersion;
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
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public static TopicConfig defaults() {
        return new TopicConfig(1000, BackpressureStrategy.REJECT, Duration.ofSeconds(2),
                Duration.ofSeconds(30), BackoffConfig.defaults(), 1);
    }
}
