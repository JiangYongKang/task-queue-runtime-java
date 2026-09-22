package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/**
 * 退避重试策略：第 n 次重试（n 从 1 起）的延迟为
 * {@code min(baseDelayMillis * multiplier^(n-1), maxDelayMillis)}。
 *
 * @param maxAttempts     最大尝试次数（含首次投递）；0 表示不重试
 * @param baseDelayMillis 首次重试基础延迟
 * @param multiplier      退避倍数
 * @param maxDelayMillis  单次延迟上限
 */
public record RetryPolicy(int maxAttempts, long baseDelayMillis, double multiplier, long maxDelayMillis) {

    public RetryPolicy {
        if (maxAttempts < 0) {
            throw new IllegalArgumentException("maxAttempts must be >= 0");
        }
        if (baseDelayMillis < 0L || maxDelayMillis < 0L) {
            throw new IllegalArgumentException("delay must be >= 0");
        }
        if (multiplier < 1.0d) {
            throw new IllegalArgumentException("multiplier must be >= 1.0");
        }
        if (maxAttempts > 0 && maxDelayMillis < baseDelayMillis) {
            throw new IllegalArgumentException("maxDelayMillis must be >= baseDelayMillis");
        }
    }

    public static RetryPolicy none() {
        return new RetryPolicy(0, 0L, 1.0d, 0L);
    }

    /**
     * 第 {@code attempt} 次重试对应的延迟。attempt=1 表示首次失败后的第一次重试。
     */
    public long delayMillisFor(int attempt) {
        if (attempt < 1) {
            return baseDelayMillis;
        }
        double raw = baseDelayMillis * Math.pow(multiplier, attempt - 1);
        if (raw >= maxDelayMillis || Double.isInfinite(raw) || Double.isNaN(raw)) {
            return maxDelayMillis;
        }
        return (long) raw;
    }
}
