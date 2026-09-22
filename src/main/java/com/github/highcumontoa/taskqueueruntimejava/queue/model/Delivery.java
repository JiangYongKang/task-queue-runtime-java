package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/** 一次投递：消息 + 该组视角下的租约。 */
public record Delivery(QueueMessage message, Lease lease) {
}
