package com.github.highcumontoa.taskqueueruntimejava.model;

/** 消息在某个消费者组内的投递状态。 */
public enum GroupDeliveryState {
    AVAILABLE,
    INFLIGHT,
    COMMITTED
}
