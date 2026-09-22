package com.github.highcumontoa.taskqueueruntimejava.model;

/**
 * 提交结果：
 * COMMITTED 首次提交成功；
 * ALREADY_COMMITTED 重复提交（幂等成功，非首次）；
 * DUPLICATE_DELIVERY 针对已失效的旧投递（已被超时重投）提交，被拒绝且可区分。
 */
public enum CommitOutcome {
    COMMITTED,
    ALREADY_COMMITTED,
    DUPLICATE_DELIVERY
}
