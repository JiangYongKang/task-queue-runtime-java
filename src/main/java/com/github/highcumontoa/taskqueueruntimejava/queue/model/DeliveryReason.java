package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/**
 * 投递原因，用于区分“合法重试”与“重复投递”。
 */
public enum DeliveryReason {
    /** 首次投递 */
    INITIAL,
    /** 消费方显式 nack（可重试）后的合法重试 */
    RETRY_AFTER_NACK,
    /** 可见性超时后租约被回收的重新投递（合法重试的一种） */
    REDISPATCH_AFTER_VISIBILITY_TIMEOUT,
    /** 运行时优雅关闭时交还在途消息后的重新投递 */
    REDISPATCH_AFTER_SHUTDOWN,
    /** 管理员回退位点后的重放 */
    REPLAY
}
