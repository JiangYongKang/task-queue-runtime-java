package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/**
 * 可观测事件（测试可监听以稳定断言）。日志统一输出 messageId/group/offset/attempt。
 */
public record QueueEvent(Kind kind,
                         String topic,
                         String group,
                         long offset,
                         String messageId,
                         int attempt,
                         String detail) {
    public enum Kind {
        PRODUCED, DELIVERED, COMMITTED, DUPLICATE_DELIVERY, DUPLICATE_COMMIT,
        NACK_RETRY, VISIBILITY_TIMEOUT_REDISPATCH, DEAD_LETTER,
        OFFSET_RESET, REJECTED, SHUTDOWN_RECLAIM
    }
}
