package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/** 否定确认（nack）结果结论。 */
public enum NackOutcome {
    RETRY_SCHEDULED,
    DEAD_LETTER
}
