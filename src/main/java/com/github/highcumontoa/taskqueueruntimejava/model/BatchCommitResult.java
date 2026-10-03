package com.github.highcumontoa.taskqueueruntimejava.model;

import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;

import java.util.List;

/**
 * 批量提交结果：逐条结论可区分。成功的提交不因同批失败而回退；
 * 失败条目携带稳定错误码（如 DELIVERY_STALE / DELIVERY_NOT_FOUND）。
 */
public final class BatchCommitResult {

    /** 单条提交结论。 */
    public record Item(String deliveryId, CommitOutcome outcome, String messageId, long offset,
                       ErrorCode errorCode, String error) {
        public static Item ok(String deliveryId, CommitResult result) {
            return new Item(deliveryId, result.getOutcome(), result.getMessageId(),
                    result.getOffset(), null, null);
        }

        public static Item failed(String deliveryId, ErrorCode code, String error) {
            return new Item(deliveryId, null, null, -1, code, error);
        }

        public boolean isSuccess() {
            return outcome != null;
        }
    }

    private final List<Item> items;
    private final int committedCount;
    private final int failedCount;

    public BatchCommitResult(List<Item> items) {
        this.items = List.copyOf(items);
        int ok = 0;
        for (Item item : items) {
            if (item.isSuccess()) {
                ok++;
            }
        }
        this.committedCount = ok;
        this.failedCount = items.size() - ok;
    }

    public List<Item> getItems() { return items; }
    public int getCommittedCount() { return committedCount; }
    public int getFailedCount() { return failedCount; }
}
