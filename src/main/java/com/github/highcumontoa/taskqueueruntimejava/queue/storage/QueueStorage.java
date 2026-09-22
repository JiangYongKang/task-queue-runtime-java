package com.github.highcumontoa.taskqueueruntimejava.queue.storage;

import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupState;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Principal;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.QueueMessage;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.TopicSettings;

import java.util.Set;

/**
 * 队列后端存储抽象。内存后端与本地可恢复后端实现相同语义。
 * 实现类必须保证各方法在并发调用下自身线程安全；跨多方法的原子性由运行时加锁保证。
 */
public interface QueueStorage {

    void createTopic(String topic, TopicSettings settings);

    void createGroup(String topic, String group, GroupSettings settings);

    TopicSettings topicSettings(String topic);

    GroupSettings groupSettings(String topic, String group);

    Set<String> listTopics();

    Set<String> listGroups(String topic);

    void registerPrincipal(Principal principal);

    Principal principal(String token);

    /** 在主题日志末尾追加消息，返回分配的位点。 */
    long appendMessage(String topic, QueueMessage message);

    /** 读取消息；位点已被保留策略淘汰时返回 null。 */
    QueueMessage readMessage(String topic, long offset);

    /** 主题下一个将分配的位点。 */
    long nextOffset(String topic);

    /** 主题日志当前保留的最早位点。 */
    long logStartOffset(String topic);

    /** 主题当前保留的消息数（用于有界深度判定）。 */
    int depth(String topic);

    /** 查询生产者幂等键已对应的位点；不存在返回 null。 */
    Long producerKeyOffset(String topic, String idempotencyKey);

    /** 记录生产者幂等键 -> 位点（有界，超出容量按最久未访问淘汰）。 */
    void putProducerKey(String topic, String idempotencyKey, long offset);

    /** 淘汰早于 retainBeforeOffset 的日志消息（所有已存在分组位点均安全之后才可调用）。 */
    void evictMessagesBefore(String topic, long retainBeforeOffset);

    GroupState loadGroupState(String topic, String group);

    void saveGroupState(GroupState state);

    /** 是否为可恢复（持久化）后端。 */
    boolean isRecoverable();

    /** 关闭并释放资源。 */
    void close();
}
