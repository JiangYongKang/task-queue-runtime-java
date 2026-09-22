package com.github.highcumontoa.taskqueueruntimejava.model;

import java.time.Duration;

/**
 * 可配置退避策略：第 n 次失败后的等待 = min(initial * multiplier^(n-1), max)。
 * maxAttempts 为最大失败次数，超过即进入死信。
 */
public final class BackoffConfig {
    private Duration initial;
    private double multiplier;
    private Duration max;
    private int maxAttempts;

    public BackoffConfig() {
    }

    public BackoffConfig(Duration initial, double multiplier, Duration max, int maxAttempts) {
        this.initial = initial;
        this.multiplier = multiplier;
        this.max = max;
        this.maxAttempts = maxAttempts;
    }

    public Duration delayForAttempt(int failedAttempts) {
        if (failedAttempts <= 0) {
            return Duration.ZERO;
        }
        double factor = Math.pow(multiplier, failedAttempts - 1);
        long millis = (long) (initial.toMillis() * factor);
        return Duration.ofMillis(Math.min(millis, max.toMillis()));
    }

    public Duration getInitial() { return initial; }
    public void setInitial(Duration initial) { this.initial = initial; }
    public double getMultiplier() { return multiplier; }
    public void setMultiplier(double multiplier) { this.multiplier = multiplier; }
    public Duration getMax() { return max; }
    public void setMax(Duration max) { this.max = max; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public static BackoffConfig defaults() {
        return new BackoffConfig(Duration.ofMillis(100), 2.0, Duration.ofSeconds(5), 3);
    }
}
