package com.github.highcumontoa.taskqueueruntimejava.model;

/** 有界队列积压时的生产侧策略：立即拒绝或有界等待后超时。 */
public enum BackpressureStrategy {
    REJECT,
    WAIT
}
