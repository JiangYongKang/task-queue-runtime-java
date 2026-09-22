package com.github.highcumontoa.taskqueueruntimejava.queue.config;

import com.github.highcumontoa.taskqueueruntimejava.queue.model.BackpressurePolicy;

/**
 * 运行时配置。
 *
 * @param defaultTopicMaxDepth          主题默认容量上限
 * @param defaultBackpressurePolicy     默认背压策略
 * @param defaultVisibilityTimeoutMillis 默认可见性超时
 * @param defaultProduceWaitMillis      DELAY 策略下生产默认最大等待
 * @param leaseReaperIntervalMillis     在途租约回收线程检查间隔
 * @param idempotencyRetention          已处理幂等键的有界保留数量
 */
public record QueueRuntimeProperties(BackendType backendType,
                                     String dataDirectory,
                                     int defaultTopicMaxDepth,
                                     BackpressurePolicy defaultBackpressurePolicy,
                                     long defaultVisibilityTimeoutMillis,
                                     long defaultProduceWaitMillis,
                                     long leaseReaperIntervalMillis,
                                     int idempotencyRetention) {

    public enum BackendType { MEMORY, LOCAL_FILE }

    public static QueueRuntimeProperties defaults() {
        return new QueueRuntimeProperties(
                BackendType.MEMORY,
                "build/taskqueue-data",
                10_000,
                BackpressurePolicy.REJECT,
                30_000L,
                1_000L,
                100L,
                10_000);
    }
}
