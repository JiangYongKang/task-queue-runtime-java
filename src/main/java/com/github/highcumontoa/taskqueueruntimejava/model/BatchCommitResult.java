package com.github.highcumontoa.taskqueueruntimejava.model;

import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;

import java.util.ArrayList;
import java.util.List;

/**
 * 批量确认结果：逐条结论，部分失败不影响同批已成功提交。
 * outcome 为 COMMITTED / ALREADY_COMMITTED 表示成功；
 * 失败时 outcome 为 null 且 errorCode 给出稳定原因
 * （如 DELIVERY_STALE、DELIVERY_NOT_FOUND、CROSS_GROUP_COMMIT_REJECTED）。
 */
public final class BatchCommitResult {

    public static final class Item {
        private final String deliveryId;
        private final CommitOutcome outcome;
        private final String messageId;
        private final long offset;
        private final ErrorCode errorCode;
        private final String error;

        public Item(String deliveryId, CommitOutcome outcome, String messageId, long offset,
                    ErrorCode errorCode, String error) {
            this.deliveryId = deliveryId;
            this.outcome = outcome;
            this.messageId = messageId;
            this.offset = offset;
            this.errorCode = errorCode;
            this.error = error;
        }

        public String getDeliveryId() { return deliveryId; }
        public CommitOutcome getOutcome() { return outcome; }
        public String getMessageId() { return messageId; }
        public long getOffset() { return offset; }
        public ErrorCode getErrorCode() { return errorCode; }
        public String getError() { return error; }
        public boolean isSuccess() { return outcome != null; }
    }

    private final List<Item> items = new ArrayList<>();

    public List<Item> getItems() { return items; }

    public void add(Item item) { items.add(item); }

    public long successCount() {
        return items.stream().filter(Item::isSuccess).count();
    }

    public long failureCount() {
        return items.stream().filter(i -> !i.isSuccess()).count();
    }
}
