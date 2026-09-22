package com.github.highcumontoa.taskqueueruntimejava.backend;

import com.github.highcumontoa.taskqueueruntimejava.model.TopicState;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * 队列后端存储抽象。内存后端与本地可恢复后端实现同一语义。
 * 所有变更必须在主题级互斥下通过 mutate 完成，保证并发安全与持久化原子性。
 */
public interface QueueBackend {

    /** 创建主题；已存在则抛出 TOPIC_ALREADY_EXISTS。 */
    void createTopic(String topic, TopicState state);

    /** 主题是否存在。 */
    boolean topicExists(String topic);

    /** 读取主题快照（不持锁，仅用于查询）。 */
    TopicState readTopic(String topic);

    /** 列出全部主题。 */
    List<String> listTopics();

    /**
     * 在主题级锁内读取并变更主题状态；函数返回 null 表示放弃变更，
     * 返回值原样返回。持久化在锁内、提交内存状态前原子完成。
     */
    <T> T mutate(String topic, Function<TopicState, T> action);

    /**
     * 在主题级锁上等待容量条件（WAIT 背压策略使用）。
     * 等待期间会释放主题锁，条件满足或超时前被唤醒都会重新抢锁检查，
     * 因此不会阻塞提交侧释放容量，杜绝持锁等死锁。
     * 必须在 {@link #mutate} 的 action 内调用（同线程重入）。
     *
     * @return 条件是否已满足（false 表示超时）
     */
    boolean awaitCapacity(String topic, long timeoutMillis, Predicate<TopicState> condition);

    /** 唤醒等待容量的生产者（提交/nack 释放容量后调用）。 */
    void signalCapacity(String topic);

    /** 关闭后端：刷盘并释放资源。 */
    void close();
}
