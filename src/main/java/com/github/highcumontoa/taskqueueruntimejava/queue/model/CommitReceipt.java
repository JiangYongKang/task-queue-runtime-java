package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/** ack 回执。 */
public record CommitReceipt(String topic,
                            String group,
                            long offset,
                            String deliveryToken,
                            CommitOutcome outcome,
                            long committedOffsetAfter) {
}
