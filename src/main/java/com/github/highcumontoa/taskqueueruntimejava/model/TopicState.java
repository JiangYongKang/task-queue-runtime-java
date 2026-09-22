package com.github.highcumontoa.taskqueueruntimejava.model;

import com.github.highcumontoa.taskqueueruntimejava.backend.GroupState;
import com.github.highcumontoa.taskqueueruntimejava.backend.GroupMessageState;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 主题的完整持久化状态：消息日志（按 offset 顺序）、
 * 各组状态、死信列表、生产者幂等键映射。
 * formatVersion 用于旧格式识别：仅受支持版本可加载。
 */
public final class TopicState {
    public static final int SUPPORTED_FORMAT_VERSION = 1;

    private String name;
    private int formatVersion = SUPPORTED_FORMAT_VERSION;
    private TopicConfig config;
    private long nextOffset;
    private final List<QueueMessage> messages = new ArrayList<>();
    private final Map<String, GroupState> groups = new LinkedHashMap<>();
    private final List<DeadLetterRecord> deadLetters = new ArrayList<>();
    private final Map<String, String> producerKeyIndex = new LinkedHashMap<>();

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public int getFormatVersion() { return formatVersion; }
    public void setFormatVersion(int formatVersion) { this.formatVersion = formatVersion; }
    public TopicConfig getConfig() { return config; }
    public void setConfig(TopicConfig config) { this.config = config; }
    public long getNextOffset() { return nextOffset; }
    public void setNextOffset(long nextOffset) { this.nextOffset = nextOffset; }
    public List<QueueMessage> getMessages() { return messages; }
    public Map<String, GroupState> getGroups() { return groups; }
    public List<DeadLetterRecord> getDeadLetters() { return deadLetters; }
    public Map<String, String> getProducerKeyIndex() { return producerKeyIndex; }

    /** 便捷查询：某消息在某组的投递状态，不存在则创建。 */
    public GroupMessageState stateFor(String groupName, String messageId) {
        GroupState group = groups.get(groupName);
        if (group == null) {
            return null;
        }
        return group.getMessages().computeIfAbsent(messageId, k -> new GroupMessageState());
    }
}
