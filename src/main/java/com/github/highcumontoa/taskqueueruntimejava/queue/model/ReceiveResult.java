package com.github.highcumontoa.taskqueueruntimejava.queue.model;

import java.util.List;

/** 拉取结果；poll 超时时 deliveries 为空列表。 */
public record ReceiveResult(String topic, String group, List<Delivery> deliveries) {
}
