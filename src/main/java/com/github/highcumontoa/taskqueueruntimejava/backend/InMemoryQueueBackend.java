package com.github.highcumontoa.taskqueueruntimejava.backend;

import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.error.QueueException;
import com.github.highcumontoa.taskqueueruntimejava.model.TopicState;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.function.Function;

/**
 * 纯内存后端：语义基准实现。
 * mutate 在每个主题独立的 ReentrantLock 下执行，保证生产、消费、
 * 位点提交、重试/死信迁移等复合操作的原子性，杜绝半更新状态。
 */
public class InMemoryQueueBackend implements QueueBackend {

    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Condition> available = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, TopicState> topics = new ConcurrentHashMap<>();
    private volatile boolean closed;

    public InMemoryQueueBackend() {
    }

    protected ReentrantLock lockFor(String topic) {
        return locks.computeIfAbsent(topic, k -> new ReentrantLock());
    }

    private Condition conditionFor(String topic) {
        return available.computeIfAbsent(topic, k -> lockFor(topic).newCondition());
    }

    protected TopicState stored(String topic) {
        return topics.get(topic);
    }

    protected void checkOpen() {
        if (closed) {
            throw new QueueException(ErrorCode.RUNTIME_CLOSED, "运行时已关闭");
        }
    }

    @Override
    public void createTopic(String topic, TopicState state) {
        checkOpen();
        lockFor(topic).lock();
        try {
            if (topics.putIfAbsent(topic, state) != null) {
                throw new QueueException(ErrorCode.TOPIC_ALREADY_EXISTS, "主题已存在: " + topic);
            }
        } finally {
            lockFor(topic).unlock();
        }
    }

    @Override
    public boolean topicExists(String topic) {
        return topics.containsKey(topic);
    }

    @Override
    public TopicState readTopic(String topic) {
        TopicState state = topics.get(topic);
        if (state == null) {
            throw new QueueException(ErrorCode.TOPIC_NOT_FOUND, "主题不存在: " + topic);
        }
        return state;
    }

    @Override
    public List<String> listTopics() {
        return new ArrayList<>(topics.keySet());
    }

    @Override
    public <T> T mutate(String topic, Function<TopicState, T> action) {
        checkOpen();
        TopicState state = readTopic(topic);
        lockFor(topic).lock();
        try {
            return action.apply(state);
        } finally {
            lockFor(topic).unlock();
        }
    }

    @Override
    public boolean awaitCapacity(String topic, long timeoutMillis, Predicate<TopicState> condition) {
        ReentrantLock lock = lockFor(topic);
        TopicState state = readTopic(topic);
        long deadline = System.currentTimeMillis() + Math.max(0, timeoutMillis);
        lock.lock();
        try {
            while (!condition.test(state)) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return false;
                }
                try {
                    conditionFor(topic).awaitNanos(java.time.Duration.ofMillis(
                            Math.min(remaining, 50)).toNanos());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new com.github.highcumontoa.taskqueueruntimejava.error.QueueException(
                            com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode.OPERATION_TIMEOUT,
                            "背压等待被中断 topic=" + topic, e);
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void signalCapacity(String topic) {
        ReentrantLock existing = locks.get(topic);
        if (existing == null) {
            return;
        }
        existing.lock();
        try {
            Condition c = available.get(topic);
            if (c != null) {
                c.signalAll();
            }
        } finally {
            existing.unlock();
        }
    }

    @Override
    public void close() {
        closed = true;
    }
}
