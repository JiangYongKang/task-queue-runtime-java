package com.github.highcumontoa.taskqueueruntimejava.model;

/**
 * 本次投递给消费者的原因，用于区分：
 * FIRST 首次投递；RETRY 消费者显式失败后的合法重试；
 * VISIBILITY_TIMEOUT 未提交导致租约过期后的重复投递；
 * REPLAY 管理员显式位点重放。
 */
public enum DeliveryReason {
    FIRST,
    RETRY,
    VISIBILITY_TIMEOUT,
    REPLAY
}
