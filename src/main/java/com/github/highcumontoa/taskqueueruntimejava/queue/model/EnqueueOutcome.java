package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/** 生产结果结论。 */
public enum EnqueueOutcome {
    ACCEPTED,
    /** 同一生产者幂等键重复生产：返回既有消息，不产生新状态 */
    DUPLICATE_PRODUCE,
    /** 多个载荷被合并为一条批处理消息 */
    BATCHED
}
