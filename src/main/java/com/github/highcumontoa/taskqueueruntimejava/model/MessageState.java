package com.github.highcumontoa.taskqueueruntimejava.model;

/** 消息在主题中的生命周期状态。 */
public enum MessageState {
    AVAILABLE,
    INFLIGHT,
    RETRY_WAIT,
    COMMITTED,
    DEAD
}
