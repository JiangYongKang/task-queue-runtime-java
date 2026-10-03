package com.github.highcumontoa.taskqueueruntimejava.model;

/**
 * 队列消息。offset 为主题内单调递增位点（从 0 起）；
 * attempt 为消息级累计失败次数；messageId 全局唯一。
 */
public final class QueueMessage {
    private String messageId;
    private String topic;
    private long offset;
    private String body;
    private String producerKey;
    private long enqueueMillis;
    private int attempt;
    private MessageState state;
    /** 主题级终结时刻（全部组提交或进入死信）；保留期据此计算，0 表示未终结。 */
    private long terminatedAtMillis;

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }
    public String getBody() { return body; }
    public void setBody(String body) { this.body = body; }
    public String getProducerKey() { return producerKey; }
    public void setProducerKey(String producerKey) { this.producerKey = producerKey; }
    public long getEnqueueMillis() { return enqueueMillis; }
    public void setEnqueueMillis(long enqueueMillis) { this.enqueueMillis = enqueueMillis; }
    public int getAttempt() { return attempt; }
    public void setAttempt(int attempt) { this.attempt = attempt; }
    public MessageState getState() { return state; }
    public void setState(MessageState state) { this.state = state; }
    public long getTerminatedAtMillis() { return terminatedAtMillis; }
    public void setTerminatedAtMillis(long terminatedAtMillis) { this.terminatedAtMillis = terminatedAtMillis; }
}
