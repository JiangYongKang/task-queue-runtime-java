package com.github.highcumontoa.taskqueueruntimejava.model;

/** 消费者组位点信息：committed 为已提交位点上界，next 为下一个待取位点。 */
public final class OffsetInfo {
    private final String topic;
    private final String group;
    private final long committed;
    private final long next;
    private final int inflight;

    public OffsetInfo(String topic, String group, long committed, long next, int inflight) {
        this.topic = topic;
        this.group = group;
        this.committed = committed;
        this.next = next;
        this.inflight = inflight;
    }

    public String getTopic() { return topic; }
    public String getGroup() { return group; }
    public long getCommitted() { return committed; }
    public long getNext() { return next; }
    public int getInflight() { return inflight; }
}
