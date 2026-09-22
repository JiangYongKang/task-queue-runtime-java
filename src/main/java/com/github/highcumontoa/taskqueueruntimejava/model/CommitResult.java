package com.github.highcumontoa.taskqueueruntimejava.model;

/** 提交位点/消息的返回结果，可区分首次提交与重复提交。 */
public final class CommitResult {
    private final CommitOutcome outcome;
    private final String messageId;
    private final long offset;

    public CommitResult(CommitOutcome outcome, String messageId, long offset) {
        this.outcome = outcome;
        this.messageId = messageId;
        this.offset = offset;
    }

    public CommitOutcome getOutcome() { return outcome; }
    public String getMessageId() { return messageId; }
    public long getOffset() { return offset; }
}
