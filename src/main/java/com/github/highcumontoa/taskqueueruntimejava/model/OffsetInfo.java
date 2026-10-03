package com.github.highcumontoa.taskqueueruntimejava.model;

/** 消费者组位点信息：committed 为已提交位点上界（连续水位线），next 为下一个待取位点，
 *  earliestRetained 为主题当前保留的最早位点（小于它的消息已被回收）。 */
public final class OffsetInfo {
    private final String topic;
    private final String group;
    private final long committed;
    private final long next;
    private final int inflight;
    private final long earliestRetained;

    public OffsetInfo(String topic, String group, long committed, long next, int inflight,
                      long earliestRetained) {
        this.topic = topic;
        this.group = group;
        this.committed = committed;
        this.next = next;
        this.inflight = inflight;
        this.earliestRetained = earliestRetained;
    }

    public String getTopic() { return topic; }
    public String getGroup() { return group; }
    public long getCommitted() { return committed; }
    public long getNext() { return next; }
    public int getInflight() { return inflight; }
    public long getEarliestRetained() { return earliestRetained; }
}
