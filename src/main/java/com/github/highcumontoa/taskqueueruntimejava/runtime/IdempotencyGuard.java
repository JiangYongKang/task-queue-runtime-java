package com.github.highcumontoa.taskqueueruntimejava.runtime;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消费侧业务幂等登记。
 *
 * 处理流程：{@link #begin} 占位 -> 业务生效 -> {@link #commit} 固化；
 * 业务失败 {@link #release}，允许合法重试重新生效。
 * 占位状态使“正在处理”的并发重复（同消息双消费者竞态）也被短路；
 * 已固化的 key 永久命中，重复投递不会二次变更业务状态。
 */
public final class IdempotencyGuard {

    private enum Mark { INFLIGHT, APPLIED }

    private final ConcurrentHashMap<String, Mark> marks = new ConcurrentHashMap<>();

    /**
     * 尝试占位：首次返回 true；已存在占位或已生效均返回 false。
     * 调用方仅在 true 时执行业务生效步骤。
     */
    public boolean begin(String sideEffectKey) {
        return marks.putIfAbsent(sideEffectKey, Mark.INFLIGHT) == null;
    }

    /** 业务成功：固化为已生效。 */
    public void commit(String sideEffectKey) {
        marks.put(sideEffectKey, Mark.APPLIED);
    }

    /** 业务处理失败：回滚占位，允许下一次合法重试。 */
    public void release(String sideEffectKey) {
        marks.remove(sideEffectKey);
    }

    /** 是否已成功生效（用于重复投递短路判断）。 */
    public boolean isApplied(String sideEffectKey) {
        return marks.get(sideEffectKey) == Mark.APPLIED;
    }

    /** 已生效的全部幂等键（用于持久化/断言）。 */
    public Set<String> appliedKeys() {
        return Collections.unmodifiableSet(
                marks.entrySet().stream()
                        .filter(e -> e.getValue() == Mark.APPLIED)
                        .map(java.util.Map.Entry::getKey)
                        .collect(java.util.stream.Collectors.toSet()));
    }
}
