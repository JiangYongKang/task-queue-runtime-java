package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/** 在途租约：标识某分组当前对某位点的持有。 */
public record Lease(long offset,
                    String deliveryToken,
                    String consumerId,
                    long leasedAtMillis,
                    long visibleAtMillis,
                    int attempt,
                    DeliveryReason reason) {
}
