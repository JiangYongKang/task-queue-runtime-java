package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/**
 * 主题设置：容量上限、背压策略与日志保留条数。
 *
 * @param maxDepth           内存中允许保留的消息条数上限（背压阈值）
 * @param backpressurePolicy 达到上限时的策略
 * @param retentionCount     位点压缩时额外保留的最近已终态消息条数，
 *                           使这些位点在被淘汰前仍可被管理员回退重放
 */
public record TopicSettings(int maxDepth, BackpressurePolicy backpressurePolicy, int retentionCount) {

    public TopicSettings {
        if (maxDepth <= 0) {
            throw new IllegalArgumentException("maxDepth must be > 0");
        }
        if (retentionCount < 0) {
            throw new IllegalArgumentException("retentionCount must be >= 0");
        }
        if (retentionCount > maxDepth) {
            throw new IllegalArgumentException("retentionCount must be <= maxDepth");
        }
    }

    public TopicSettings(int maxDepth, BackpressurePolicy backpressurePolicy) {
        this(maxDepth, backpressurePolicy, 0);
    }

    public static TopicSettings defaults() {
        return new TopicSettings(10_000, BackpressurePolicy.REJECT, 0);
    }
}
