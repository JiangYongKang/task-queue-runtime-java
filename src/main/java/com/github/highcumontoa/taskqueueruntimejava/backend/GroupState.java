package com.github.highcumontoa.taskqueueruntimejava.backend;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 消费者组状态：committedOffset 为已提交位点上界（初始 -1）；
 * messages 按 messageId 保存每条消息在本组的投递状态。
 */
public final class GroupState {
    private String name;
    private long committedOffset = -1;
    private final Map<String, GroupMessageState> messages = new LinkedHashMap<>();

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public long getCommittedOffset() { return committedOffset; }
    public void setCommittedOffset(long committedOffset) { this.committedOffset = committedOffset; }
    public Map<String, GroupMessageState> getMessages() { return messages; }
}
