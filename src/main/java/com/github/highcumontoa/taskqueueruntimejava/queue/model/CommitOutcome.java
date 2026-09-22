package com.github.highcumontoa.taskqueueruntimejava.queue.model;

/** 提交（ack）结果结论。 */
public enum CommitOutcome {
    COMMITTED,
    /** 同一投递令牌重复提交：幂等成功 */
    ALREADY_COMMITTED,
    /** 业务幂等键此前已生效，本次为重复处理 */
    DUPLICATE_DELIVERY
}
