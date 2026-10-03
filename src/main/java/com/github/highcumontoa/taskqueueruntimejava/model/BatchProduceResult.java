package com.github.highcumontoa.taskqueueruntimejava.model;

import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;

import java.util.ArrayList;
import java.util.List;

/**
 * 批量生产结果：逐条结论，部分失败不回退已成功项。
 * 每条状态可区分：STORED（新入库）/ DUPLICATE（幂等命中）/ REJECTED（失败，
 * errorCode 给出稳定原因，如 QUEUE_FULL、BACKPRESSURE_TIMEOUT）。
 */
public final class BatchProduceResult {

    public enum ItemStatus { STORED, DUPLICATE, REJECTED }

    public static final class Item {
        private final int index;
        private final ItemStatus status;
        private final String messageId;
        private final long offset;
        private final ErrorCode errorCode;
        private final String error;

        public Item(int index, ItemStatus status, String messageId, long offset,
                    ErrorCode errorCode, String error) {
            this.index = index;
            this.status = status;
            this.messageId = messageId;
            this.offset = offset;
            this.errorCode = errorCode;
            this.error = error;
        }

        public int getIndex() { return index; }
        public ItemStatus getStatus() { return status; }
        public String getMessageId() { return messageId; }
        public long getOffset() { return offset; }
        public ErrorCode getErrorCode() { return errorCode; }
        public String getError() { return error; }
    }

    private final List<Item> items = new ArrayList<>();

    public List<Item> getItems() { return items; }

    public void add(Item item) { items.add(item); }

    public long storedCount() {
        return items.stream().filter(i -> i.status == ItemStatus.STORED).count();
    }

    public long duplicateCount() {
        return items.stream().filter(i -> i.status == ItemStatus.DUPLICATE).count();
    }

    public long rejectedCount() {
        return items.stream().filter(i -> i.status == ItemStatus.REJECTED).count();
    }
}
