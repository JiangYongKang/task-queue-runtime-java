package com.github.highcumontoa.taskqueueruntimejava.queue.runtime;

import com.github.highcumontoa.taskqueueruntimejava.queue.model.DeadLetterRecord;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.DeliveryReason;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupState;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Lease;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * {@link GroupState} 的运行时可变视图。所有消费侧状态变更都在该对象上进行，
 * 最后通过 {@link #snapshot()} 一次性写回存储，避免不可变 record 重建导致的更新丢失。
 */
final class MutableGroupState {

    final String topic;
    final String group;
    long committedOffset;
    long nextOffset;
    final Map<Long, Integer> attempts;
    final Map<Long, Long> availableAt;
    final Map<Long, String> idempotencyKeys;
    final Map<Long, DeliveryReason> pendingReasons;
    final Map<Long, Lease> leases;
    final Set<Long> ackedOffsets;
    final Map<String, Long> committedTokens;
    final Map<String, Long> processedKeys;
    final List<DeadLetterRecord> deadLetters;

    private MutableGroupState(GroupState s) {
        this.topic = s.topic();
        this.group = s.group();
        this.committedOffset = s.committedOffset();
        this.nextOffset = s.nextOffset();
        this.attempts = new TreeMap<>(s.perOffsetAttempts());
        this.availableAt = new TreeMap<>(s.perOffsetAvailableAt());
        this.idempotencyKeys = new TreeMap<>(s.perOffsetIdempotencyKey());
        this.pendingReasons = new TreeMap<>(s.pendingReasons());
        this.leases = new TreeMap<>(s.leases());
        this.ackedOffsets = new TreeSet<>(s.ackedOffsets());
        this.committedTokens = new java.util.LinkedHashMap<>(s.committedTokens());
        this.processedKeys = new java.util.LinkedHashMap<>(s.processedKeys());
        this.deadLetters = new ArrayList<>(s.deadLetters());
    }

    static MutableGroupState load(com.github.highcumontoa.taskqueueruntimejava.queue.storage.QueueStorage storage,
                                  String topic, String group) {
        return new MutableGroupState(storage.loadGroupState(topic, group));
    }

    GroupState snapshot() {
        return new GroupState(topic, group, committedOffset, nextOffset,
                attempts, availableAt, idempotencyKeys, pendingReasons, leases,
                ackedOffsets, committedTokens, processedKeys, deadLetters);
    }

    void save(com.github.highcumontoa.taskqueueruntimejava.queue.storage.QueueStorage storage) {
        storage.saveGroupState(snapshot());
    }
}
