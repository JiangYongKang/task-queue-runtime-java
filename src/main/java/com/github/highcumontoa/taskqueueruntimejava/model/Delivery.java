package com.github.highcumontoa.taskqueueruntimejava.model;

/**
 * 一次投递句柄。deliveryId 唯一标识本次投递；
 * reason 表明投递原因；attempt 为第几次尝试；
 * visibleUntilMillis 为可见性超时时刻。
 */
public final class Delivery {
    private String deliveryId;
    private String messageId;
    private String topic;
    private String group;
    private long offset;
    private String body;
    private int attempt;
    private DeliveryReason reason;
    private long visibleUntilMillis;
    private long deliveredAtMillis;

    public String getDeliveryId() { return deliveryId; }
    public void setDeliveryId(String deliveryId) { this.deliveryId = deliveryId; }
    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public String getGroup() { return group; }
    public void setGroup(String group) { this.group = group; }
    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }
    public String getBody() { return body; }
    public void setBody(String body) { this.body = body; }
    public int getAttempt() { return attempt; }
    public void setAttempt(int attempt) { this.attempt = attempt; }
    public DeliveryReason getReason() { return reason; }
    public void setReason(DeliveryReason reason) { this.reason = reason; }
    public long getVisibleUntilMillis() { return visibleUntilMillis; }
    public void setVisibleUntilMillis(long visibleUntilMillis) { this.visibleUntilMillis = visibleUntilMillis; }
    public long getDeliveredAtMillis() { return deliveredAtMillis; }
    public void setDeliveredAtMillis(long deliveredAtMillis) { this.deliveredAtMillis = deliveredAtMillis; }
}
