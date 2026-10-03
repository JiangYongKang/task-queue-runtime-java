package com.github.highcumontoa.taskqueueruntimejava.model;

/**
 * 有界队列积压时的生产侧策略：立即拒绝、有界等待后超时，或成批接收。
 * BATCH 策略下单条 produce 与 REJECT 行为一致；批量生产按可用容量
 * 尽量接收，放不下的条目在批结果中逐条标记失败，不回退已接收部分。
 */
public enum BackpressureStrategy {
    REJECT,
    WAIT,
    BATCH
}
