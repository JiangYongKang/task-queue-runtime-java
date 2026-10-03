package com.github.highcumontoa.taskqueueruntimejava.model;

import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;

import java.util.List;

/**
 * 批量生产结果：逐条结论可区分。部分失败不回退已成功条目。
 * accepted 含去重命中（DUPLICATE）的条目。
 */
public final class BatchProduceResult {

    public enum ItemStatus {
        /** 新消息已接收。 */
        ACCEPTED,
        /** producerKey 去重命中既有消息，未重复接收。 */
        DUPLICATE,
        /** 该条被拒绝（如容量不足），不影响同批其它条目。 */
        REJECTED
    }

    /** 单条结果：成功时带 messageId/offset，失败时带 errorCode。 */
    public record Item(int index, ItemStatus status, String messageId, long offset,
                       ErrorCode errorCode, String error) {
        public static Item accepted(int index, String messageId, long offset) {
            return new Item(index, ItemStatus.ACCEPTED, messageId, offset, null, null);
        }

        public static Item duplicate(int index, String messageId, long offset) {
            return new Item(index, ItemStatus.DUPLICATE, messageId, offset, null, null);
        }

        public static Item rejected(int index, ErrorCode code, String error) {
            return new Item(index, ItemStatus.REJECTED, null, -1, code, error);
        }
    }

    private final List<Item> items;
    private final int acceptedCount;
    private final int rejectedCount;

    public BatchProduceResult(List<Item> items) {
        this.items = List.copyOf(items);
        int accepted = 0;
        for (Item item : items) {
            if (item.status() != ItemStatus.REJECTED) {
                accepted++;
            }
        }
        this.acceptedCount = accepted;
        this.rejectedCount = items.size() - accepted;
    }

    public List<Item> getItems() { return items; }
    public int getAcceptedCount() { return acceptedCount; }
    public int getRejectedCount() { return rejectedCount; }
}
