package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/**
 * 一条消息针对某个消费者组的状态。
 */
public enum MessageState {
    READY,
    IN_FLIGHT,
    COMMITTED,
    DEAD
}
