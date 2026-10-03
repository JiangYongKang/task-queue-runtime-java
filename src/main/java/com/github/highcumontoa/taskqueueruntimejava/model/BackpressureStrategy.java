package com.github.highcumontoa.taskqueueruntimejava.model;

/**
 * 有界队列积压时的生产侧策略：立即拒绝、有界等待后超时，
 * 或批量（BATCH：面向批量导入/结算场景，配合批量生产/批量确认 API
 * 按可配置批大小成批处理；单条 produce 在 BATCH 下退化为有界等待）。
 */
public enum BackpressureStrategy {
    REJECT,
    WAIT,
    BATCH
}
