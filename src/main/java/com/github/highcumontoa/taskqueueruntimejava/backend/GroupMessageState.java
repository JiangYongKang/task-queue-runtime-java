package com.github.highcumontoa.taskqueueruntimejava.backend;

import com.github.highcumontoa.taskqueueruntimejava.model.GroupDeliveryState;

/**
 * 某条消息在某个消费者组中的投递状态（按组独立）。
 * currentDeliveryId 为当前生效投递标识；lastDeliveryId 保留最近一次
 * （含已提交）的投递标识，用于识别重复提交。
 * timeoutReclaimed 表示上一轮因可见性超时被回收；replayPending 表示
 * 由位点重放重新置为可投递——二者决定下次投递的可区分原因。
 */
public final class GroupMessageState {
    private GroupDeliveryState state = GroupDeliveryState.AVAILABLE;
    private String currentDeliveryId;
    private String lastDeliveryId;
    private int attempts;
    private long visibleUntilMillis;
    private long availableAfterMillis;
    private long lastDispatchedMillis;
    private boolean timeoutReclaimed;
    private boolean replayPending;

    public GroupDeliveryState getState() { return state; }
    public void setState(GroupDeliveryState state) { this.state = state; }
    public String getCurrentDeliveryId() { return currentDeliveryId; }
    public void setCurrentDeliveryId(String currentDeliveryId) { this.currentDeliveryId = currentDeliveryId; }
    public String getLastDeliveryId() { return lastDeliveryId; }
    public void setLastDeliveryId(String lastDeliveryId) { this.lastDeliveryId = lastDeliveryId; }
    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }
    public long getVisibleUntilMillis() { return visibleUntilMillis; }
    public void setVisibleUntilMillis(long visibleUntilMillis) { this.visibleUntilMillis = visibleUntilMillis; }
    public long getAvailableAfterMillis() { return availableAfterMillis; }
    public void setAvailableAfterMillis(long availableAfterMillis) { this.availableAfterMillis = availableAfterMillis; }
    public long getLastDispatchedMillis() { return lastDispatchedMillis; }
    public void setLastDispatchedMillis(long lastDispatchedMillis) { this.lastDispatchedMillis = lastDispatchedMillis; }
    public boolean isTimeoutReclaimed() { return timeoutReclaimed; }
    public void setTimeoutReclaimed(boolean timeoutReclaimed) { this.timeoutReclaimed = timeoutReclaimed; }
    public boolean isReplayPending() { return replayPending; }
    public void setReplayPending(boolean replayPending) { this.replayPending = replayPending; }
}
