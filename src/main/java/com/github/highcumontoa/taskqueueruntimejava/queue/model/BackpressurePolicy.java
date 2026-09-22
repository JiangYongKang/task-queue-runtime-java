package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/**
 * 有界队列达到容量时的生产侧策略。
 */
public enum BackpressurePolicy {
    /** 立即拒绝，返回 QUEUE_FULL */
    REJECT,
    /** 阻塞等待容量，超时返回 PRODUCE_TIMEOUT */
    DELAY,
    /** 将多个载荷合并为一条批处理消息 */
    BATCH
}
