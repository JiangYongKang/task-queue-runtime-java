package com.github.highcumontoa.taskqueueruntimejava.queue.model;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 某（主题, 消费者组）的全部持久化消费状态。
 *
 * @param committedOffset       已提交位点：小于该值的位点均已终态（已提交或死信）
 * @param nextOffset            该组下一个待分配位点（跟随主题日志末尾）
 * @param perOffsetAttempts     位点 -> 已尝试次数
 * @param perOffsetAvailableAt  位点 -> 变为可见的时间戳（退避/可见性超时）
 * @param perOffsetIdempotencyKey 位点 -> 生产时携带的业务幂等键（可空）
 * @param pendingReasons        位点 -> 下一次投递原因
 * @param leases                当前在途租约（位点 -> 租约）
 * @param ackedOffsets          已确认但可能因乱序提交尚未并入 committedOffset 的位点
 * @param committedTokens       已提交投递令牌 -> 位点（有界 LRU，支撑重复提交幂等）
 * @param processedKeys         已生效业务幂等键 -> 首次位点（有界 LRU，支撑重复投递去重）
 * @param deadLetters           死信记录（可查询）
 */
public record GroupState(String topic,
                         String group,
                         long committedOffset,
                         long nextOffset,
                         Map<Long, Integer> perOffsetAttempts,
                         Map<Long, Long> perOffsetAvailableAt,
                         Map<Long, String> perOffsetIdempotencyKey,
                         Map<Long, DeliveryReason> pendingReasons,
                         Map<Long, Lease> leases,
                         Set<Long> ackedOffsets,
                         Map<String, Long> committedTokens,
                         Map<String, Long> processedKeys,
                         List<DeadLetterRecord> deadLetters) {
}
