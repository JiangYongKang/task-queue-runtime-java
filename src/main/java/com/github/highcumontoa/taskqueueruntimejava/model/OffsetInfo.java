package com.github.highcumontoa.taskqueueruntimejava.model;

/** 消费者组位点信息：committed 为已提交位点上界（连续水位线），next 为下一个待取位点，
 *  earliestRetained 为主题当前保留的最早位点（小于它的消息已被回收）。
 *  当本对象来自一次显式重放时，replayed=true，且 replayFrom/replayHigh/resetCount
 *  描述本次实际重置并将重新投递的已确认消息区间；普通位点查询时 replayed=false。 */
public final class OffsetInfo {
    private final String topic;
    private final String group;
    private final long committed;
    private final long next;
    private final int inflight;
    private final long earliestRetained;
    private final boolean replayed;
    private final long replayFrom;
    private final long replayHigh;
    private final int resetCount;

    public OffsetInfo(String topic, String group, long committed, long next, int inflight,
                      long earliestRetained) {
        this(topic, group, committed, next, inflight, earliestRetained,
                false, -1L, -1L, 0);
    }

    public OffsetInfo(String topic, String group, long committed, long next, int inflight,
                      long earliestRetained, boolean replayed,
                      long replayFrom, long replayHigh, int resetCount) {
        this.topic = topic;
        this.group = group;
        this.committed = committed;
        this.next = next;
        this.inflight = inflight;
        this.earliestRetained = earliestRetained;
        this.replayed = replayed;
        this.replayFrom = replayFrom;
        this.replayHigh = replayHigh;
        this.resetCount = resetCount;
    }

    public String getTopic() { return topic; }
    public String getGroup() { return group; }
    public long getCommitted() { return committed; }
    public long getNext() { return next; }
    public int getInflight() { return inflight; }
    public long getEarliestRetained() { return earliestRetained; }
    /** 本结论是否来自一次成功的显式重放（与普通位点查询区分）。 */
    public boolean isReplayed() { return replayed; }
    /** 重放目标位点（区间下界，不含）；-1 表示从头。 */
    public long getReplayFrom() { return replayFrom; }
    /** 重放时本组最高已确认位点（区间上界，含）。 */
    public long getReplayHigh() { return replayHigh; }
    /** 本次实际重置为可投递的已确认消息条数。 */
    public int getResetCount() { return resetCount; }
}
