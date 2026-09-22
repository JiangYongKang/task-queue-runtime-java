package com.github.highcumontoa.taskqueueruntimejava.model;

/**
 * 死信记录：重试耗尽（RETRIES_EXHAUSTED）或被显式标记为不可重试
 * （NON_RETRYABLE）时进入，可按主题查询，不会被静默丢弃。
 */
public final class DeadLetterRecord {
    public enum Cause { RETRIES_EXHAUSTED, NON_RETRYABLE }

    private String messageId;
    private String topic;
    private long offset;
    private String body;
    private int attempts;
    private Cause cause;
    private long deadAtMillis;
    private String lastError;

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }
    public String getBody() { return body; }
    public void setBody(String body) { this.body = body; }
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public Cause getCause() { return cause; }
    public void setCause(Cause cause) { this.cause = cause; }
    public long getDeadAtMillis() { return deadAtMillis; }
    public void setDeadAtMillis(long deadAtMillis) { this.deadAtMillis = deadAtMillis; }
    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }
}
