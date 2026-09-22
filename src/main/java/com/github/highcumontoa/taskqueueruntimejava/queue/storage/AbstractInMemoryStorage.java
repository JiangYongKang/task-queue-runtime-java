package com.github.highcumontoa.taskqueueruntimejava.queue.storage;

import com.github.highcumontoa.taskqueueruntimejava.queue.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.QueueRuntimeException;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.DeadLetterRecord;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupState;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Lease;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Principal;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.QueueMessage;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.TopicSettings;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 纯内存后端。所有方法 synchronized，保证多线程下存储自身一致；
 * 跨方法原子性由 {@code TaskQueueRuntime} 的全局锁保证。
 * 本地文件后端继承本类并在变更点上增加同步刷盘。
 */
public class AbstractInMemoryStorage implements QueueStorage {

    protected static final class TopicStore {
        TopicSettings settings;
        long nextOffset;
        long logStartOffset;
        final TreeMap<Long, QueueMessage> messages = new TreeMap<>();
        final Map<String, GroupStore> groups = new LinkedHashMap<>();
        /** 生产者业务幂等键 -> 已分配位点（有界 LRU）。 */
        final Map<String, Long> producerKeys = new LinkedHashMap<>(16, 0.75f, true);
    }

    protected static final class GroupStore {
        GroupSettings settings;
        long committedOffset;
        long nextOffset;
        final Map<Long, Integer> attempts = new TreeMap<>();
        final Map<Long, Long> availableAt = new TreeMap<>();
        final Map<Long, String> idempotencyKeys = new TreeMap<>();
        final Map<Long, com.github.highcumontoa.taskqueueruntimejava.queue.model.DeliveryReason>
                pendingReasons = new TreeMap<>();
        final Map<Long, Lease> leases = new TreeMap<>();
        final java.util.Set<Long> ackedOffsets = new java.util.TreeSet<>();
        final Map<String, Long> committedTokens = new LinkedHashMap<>();
        final Map<String, Long> processedKeys = new LinkedHashMap<>();
        final List<DeadLetterRecord> deadLetters = new ArrayList<>();

        GroupState snapshot(String topic, String group) {
            return new GroupState(topic, group, committedOffset, nextOffset,
                    new TreeMap<>(attempts), new TreeMap<>(availableAt),
                    new TreeMap<>(idempotencyKeys), new TreeMap<>(pendingReasons),
                    new TreeMap<>(leases), new java.util.TreeSet<>(ackedOffsets),
                    new LinkedHashMap<>(committedTokens), new LinkedHashMap<>(processedKeys),
                    List.copyOf(deadLetters));
        }
    }

    protected final Map<String, TopicStore> topics = new LinkedHashMap<>();
    protected final Map<String, Principal> principals = new ConcurrentHashMap<>();

    @Override
    public synchronized void createTopic(String topic, TopicSettings settings) {
        if (topics.containsKey(topic)) {
            return;
        }
        TopicStore store = new TopicStore();
        store.settings = settings;
        topics.put(topic, store);
        afterTopicCreated(topic, settings);
    }

    @Override
    public synchronized void createGroup(String topic, String group, GroupSettings settings) {
        TopicStore topicStore = requireTopic(topic);
        if (!topicStore.groups.containsKey(group)) {
            GroupStore groupStore = new GroupStore();
            groupStore.settings = settings;
            groupStore.committedOffset = 0L;
            groupStore.nextOffset = 0L;
            topicStore.groups.put(group, groupStore);
            afterGroupCreated(topic, group, settings);
        }
    }

    @Override
    public synchronized TopicSettings topicSettings(String topic) {
        return requireTopic(topic).settings;
    }

    @Override
    public synchronized GroupSettings groupSettings(String topic, String group) {
        return requireGroup(topic, group).settings;
    }

    @Override
    public synchronized Set<String> listTopics() {
        return Set.copyOf(topics.keySet());
    }

    @Override
    public synchronized Set<String> listGroups(String topic) {
        return Set.copyOf(requireTopic(topic).groups.keySet());
    }

    @Override
    public void registerPrincipal(Principal principal) {
        principals.putIfAbsent(principal.token(), principal);
    }

    @Override
    public Principal principal(String token) {
        return token == null ? null : principals.get(token);
    }

    @Override
    public synchronized long appendMessage(String topic, QueueMessage message) {
        TopicStore store = requireTopic(topic);
        long offset = store.nextOffset;
        QueueMessage stored = new QueueMessage(topic, offset, message.messageId(), message.payload(),
                message.contentType(), message.headers(), message.enqueueTimeMillis(),
                message.formatVersion());
        store.messages.put(offset, stored);
        store.nextOffset = offset + 1;
        afterMessageAppended(stored);
        return offset;
    }

    @Override
    public synchronized QueueMessage readMessage(String topic, long offset) {
        TopicStore store = requireTopic(topic);
        QueueMessage message = store.messages.get(offset);
        if (message == null && offset < store.logStartOffset) {
            throw new QueueRuntimeException(ErrorCode.OFFSET_OUT_OF_RANGE,
                    "offset " + offset + " of topic '" + topic + "' has been evicted (logStartOffset="
                            + store.logStartOffset + ")");
        }
        if (message == null && offset >= store.nextOffset) {
            throw new QueueRuntimeException(ErrorCode.OFFSET_OUT_OF_RANGE,
                    "offset " + offset + " of topic '" + topic + "' does not exist yet (nextOffset="
                            + store.nextOffset + ")");
        }
        return message;
    }

    @Override
    public synchronized long nextOffset(String topic) {
        return requireTopic(topic).nextOffset;
    }

    @Override
    public synchronized long logStartOffset(String topic) {
        return requireTopic(topic).logStartOffset;
    }

    @Override
    public synchronized int depth(String topic) {
        return requireTopic(topic).messages.size();
    }

    @Override
    public synchronized Long producerKeyOffset(String topic, String idempotencyKey) {
        if (idempotencyKey == null) {
            return null;
        }
        return requireTopic(topic).producerKeys.get(idempotencyKey);
    }

    @Override
    public synchronized void putProducerKey(String topic, String idempotencyKey, long offset) {
        TopicStore store = requireTopic(topic);
        store.producerKeys.put(idempotencyKey, offset);
        trimLru(store.producerKeys, store.settings.maxDepth());
    }

    @Override
    public synchronized void evictMessagesBefore(String topic, long retainBeforeOffset) {
        TopicStore store = requireTopic(topic);
        Map<Long, QueueMessage> head = store.messages.headMap(retainBeforeOffset);
        if (head.isEmpty()) {
            return;
        }
        head.clear();
        store.logStartOffset = Math.max(store.logStartOffset, retainBeforeOffset);
        afterMessagesEvicted(topic, store.logStartOffset);
    }

    @Override
    public synchronized GroupState loadGroupState(String topic, String group) {
        return requireGroup(topic, group).snapshot(topic, group);
    }

    @Override
    public synchronized void saveGroupState(GroupState state) {
        GroupStore store = requireGroup(state.topic(), state.group());
        store.committedOffset = state.committedOffset();
        store.nextOffset = state.nextOffset();
        store.attempts.clear();
        store.attempts.putAll(state.perOffsetAttempts());
        store.availableAt.clear();
        store.availableAt.putAll(state.perOffsetAvailableAt());
        store.idempotencyKeys.clear();
        store.idempotencyKeys.putAll(state.perOffsetIdempotencyKey());
        store.pendingReasons.clear();
        store.pendingReasons.putAll(state.pendingReasons());
        store.leases.clear();
        store.leases.putAll(state.leases());
        store.ackedOffsets.clear();
        store.ackedOffsets.addAll(state.ackedOffsets());
        store.committedTokens.clear();
        store.committedTokens.putAll(state.committedTokens());
        store.processedKeys.clear();
        store.processedKeys.putAll(state.processedKeys());
        store.deadLetters.clear();
        store.deadLetters.addAll(state.deadLetters());
        afterGroupStateSaved(state);
    }

    @Override
    public boolean isRecoverable() {
        return false;
    }

    @Override
    public void close() {
    }

    // ---- 供子类扩展的持久化钩子 ----
    protected void afterTopicCreated(String topic, TopicSettings settings) { }
    protected void afterGroupCreated(String topic, String group, GroupSettings settings) { }
    protected void afterMessageAppended(QueueMessage message) { }
    protected void afterGroupStateSaved(GroupState state) { }
    protected void afterMessagesEvicted(String topic, long newLogStartOffset) { }

    static <K, V> void trimLru(Map<K, V> map, int maxEntries) {
        if (maxEntries <= 0) {
            return;
        }
        java.util.Iterator<K> it = map.keySet().iterator();
        while (map.size() > maxEntries && it.hasNext()) {
            it.next();
            it.remove();
        }
    }

    protected TopicStore requireTopic(String topic) {
        TopicStore store = topics.get(topic);
        if (store == null) {
            throw new QueueRuntimeException(ErrorCode.UNKNOWN_TOPIC, "unknown topic: " + topic);
        }
        return store;
    }

    protected GroupStore requireGroup(String topic, String group) {
        TopicStore store = requireTopic(topic);
        GroupStore groupStore = store.groups.get(group);
        if (groupStore == null) {
            throw new QueueRuntimeException(ErrorCode.UNKNOWN_GROUP,
                    "unknown consumer group: " + group + " on topic: " + topic);
        }
        return groupStore;
    }
}
