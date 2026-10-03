package com.github.highcumontoa.taskqueueruntimejava.runtime;

import com.github.highcumontoa.taskqueueruntimejava.backend.GroupMessageState;
import com.github.highcumontoa.taskqueueruntimejava.backend.GroupState;
import com.github.highcumontoa.taskqueueruntimejava.backend.QueueBackend;
import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.error.QueueException;
import com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig;
import com.github.highcumontoa.taskqueueruntimejava.model.BackpressureStrategy;
import com.github.highcumontoa.taskqueueruntimejava.model.BatchCommitResult;
import com.github.highcumontoa.taskqueueruntimejava.model.BatchItem;
import com.github.highcumontoa.taskqueueruntimejava.model.BatchProduceResult;
import com.github.highcumontoa.taskqueueruntimejava.model.CommitOutcome;
import com.github.highcumontoa.taskqueueruntimejava.model.CommitResult;
import com.github.highcumontoa.taskqueueruntimejava.model.Credential;
import com.github.highcumontoa.taskqueueruntimejava.model.DeadLetterRecord;
import com.github.highcumontoa.taskqueueruntimejava.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.model.DeliveryReason;
import com.github.highcumontoa.taskqueueruntimejava.model.GroupDeliveryState;
import com.github.highcumontoa.taskqueueruntimejava.model.MessageState;
import com.github.highcumontoa.taskqueueruntimejava.model.OffsetInfo;
import com.github.highcumontoa.taskqueueruntimejava.model.Permission;
import com.github.highcumontoa.taskqueueruntimejava.model.QueueMessage;
import com.github.highcumontoa.taskqueueruntimejava.model.TopicConfig;
import com.github.highcumontoa.taskqueueruntimejava.model.TopicState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * 运行时默认实现。全部复合操作在后端的主题级互斥锁内完成，
 * 因而“挑消息 -> 改投递状态 -> 推进位点 / 迁移死信”是原子的，
 * 并发消费、提交与重试/死信迁移不会出现半更新或双重生效。
 */
public class DefaultTaskQueueRuntime implements TaskQueueRuntime {

    private static final Logger log = LoggerFactory.getLogger(DefaultTaskQueueRuntime.class);

    private final QueueBackend backend;
    private final AccessControl accessControl;
    private final Clock clock;
    private final IdempotencyGuard idempotencyGuard;
    private final AtomicInteger inflightCount = new AtomicInteger();
    private volatile boolean closed;

    public DefaultTaskQueueRuntime(QueueBackend backend, AccessControl accessControl, Clock clock) {
        this.backend = backend;
        this.accessControl = accessControl;
        this.clock = clock;
        this.idempotencyGuard = new IdempotencyGuard();
    }

    private void checkOpen() {
        if (closed) {
            throw new QueueException(ErrorCode.RUNTIME_CLOSED, "运行时已关闭，拒绝新操作");
        }
    }

    private Credential auth(String token, String topic, Permission permission) {
        Credential credential = accessControl.authenticate(token);
        accessControl.authorize(credential, topic, permission);
        return credential;
    }

    /**
     * 回收已终结消息：按 offset 连续前缀推进，头消息终结（全组提交/死信）
     * 且（超过保留期 或 容量承压）时回收，baseOffset 单调前进。
     * 同时清理各组投递状态、生产者幂等索引与到期死信记录。
     * 容量承压下的提前回收保证容量上限真实约束长期占用；
     * 代价是被提前回收的历史不再可重放（重放会得到明确的 OFFSET_COMPACTED）。
     *
     * @return 回收的消息条数
     */
    private int compact(TopicState state) {
        long now = clock.millis();
        long retentionMillis = state.getConfig().getRetentionMillis().toMillis();
        boolean underPressure = state.getMessages().size() >= state.getConfig().getCapacity();
        int removed = 0;
        while (!state.getMessages().isEmpty()) {
            QueueMessage head = state.getMessages().get(0);
            boolean terminated = head.getState() == MessageState.COMMITTED
                    || head.getState() == MessageState.DEAD;
            if (!terminated) {
                break;
            }
            boolean eligible = now - head.getTerminatedAtMillis() >= retentionMillis
                    || underPressure;
            if (!eligible) {
                break;
            }
            state.getMessages().remove(0);
            state.setBaseOffset(state.getBaseOffset() + 1);
            for (GroupState g : state.getGroups().values()) {
                g.getMessages().remove(head.getMessageId());
            }
            if (head.getProducerKey() != null) {
                state.getProducerKeyIndex().remove(head.getProducerKey(), head.getMessageId());
            }
            removed++;
        }
        // 死信记录按保留期回收（容量承压时同样提前），避免长跑下无限积累。
        int before = state.getDeadLetters().size();
        long retention = retentionMillis;
        state.getDeadLetters().removeIf(r ->
                now - r.getDeadAtMillis() >= retention || underPressure);
        int deadRemoved = before - state.getDeadLetters().size();
        if (removed > 0 || deadRemoved > 0) {
            log.info("回收终结消息 topic={} reclaimed={} deadLettersReclaimed={} baseOffset={} retained={}",
                    state.getName(), removed, deadRemoved, state.getBaseOffset(),
                    state.getMessages().size());
        }
        return removed;
    }

    @Override
    public void registerCredential(Credential credential) {
        checkOpen();
        accessControl.register(credential);
    }

    @Override
    public void grant(String token, String topic, Permission permission) {
        checkOpen();
        accessControl.grant(token, topic, permission);
    }

    @Override
    public void createTopic(String token, String topic, TopicConfig config) {
        checkOpen();
        Credential credential = accessControl.authenticate(token);
        accessControl.authorize(credential, topic, Permission.ADMIN);
        if (topic == null || topic.isBlank()) {
            throw new QueueException(ErrorCode.BAD_REQUEST, "主题名不能为空");
        }
        TopicConfig cfg = config == null ? TopicConfig.defaults() : config;
        if (cfg.getBackpressureStrategy() == null) {
            cfg.setBackpressureStrategy(BackpressureStrategy.REJECT);
        }
        if (cfg.getBackoff() == null) {
            cfg.setBackoff(com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults());
        }
        if (cfg.getBackpressureTimeout() == null) {
            cfg.setBackpressureTimeout(Duration.ofSeconds(2));
        }
        if (cfg.getVisibilityTimeout() == null) {
            cfg.setVisibilityTimeout(Duration.ofSeconds(30));
        }
        if (cfg.getCapacity() <= 0) {
            cfg.setCapacity(1000);
        }
        if (cfg.getRetentionMillis() == null) {
            cfg.setRetentionMillis(Duration.ofMinutes(1));
        }
        if (cfg.getMaxBatchSize() <= 0
                || cfg.getMaxBatchSize() > TopicConfig.MAX_BATCH_SIZE_HARD_LIMIT) {
            int clamped = cfg.getMaxBatchSize() <= 0
                    ? 200 : TopicConfig.MAX_BATCH_SIZE_HARD_LIMIT;
            log.warn("maxBatchSize 配置越界，钳制为 {} topic={} requested={}",
                    clamped, topic, cfg.getMaxBatchSize());
            cfg.setMaxBatchSize(clamped);
        }
        TopicState state = new TopicState();
        state.setName(topic);
        state.setFormatVersion(TopicState.SUPPORTED_FORMAT_VERSION);
        state.setConfig(cfg);
        state.setNextOffset(0);
        backend.createTopic(topic, state);
        log.info("主题已创建 topic={} capacity={} formatVersion={}", topic, cfg.getCapacity(),
                cfg.getFormatVersion());
    }

    @Override
    public void createGroup(String token, String topic, String group) {
        checkOpen();
        auth(token, topic, Permission.ADMIN);
        if (group == null || group.isBlank()) {
            throw new QueueException(ErrorCode.BAD_REQUEST, "消费者组名不能为空");
        }
        backend.mutate(topic, state -> {
            if (state.getGroups().containsKey(group)) {
                throw new QueueException(ErrorCode.GROUP_ALREADY_EXISTS,
                        "消费者组已存在: " + group + " (topic=" + topic + ")");
            }
            GroupState gs = new GroupState();
            gs.setName(group);
            gs.setCommittedOffset(-1);
            state.getGroups().put(group, gs);
            log.info("消费者组已创建 topic={} group={}", topic, group);
            return null;
        });
    }

    @Override
    public ProduceReceipt produce(String token, String topic, String body, String producerKey) {
        checkOpen();
        auth(token, topic, Permission.PRODUCE);
        long backpressureTimeoutMillis = backend.readTopic(topic)
                .getConfig().getBackpressureTimeout().toMillis();
        return backend.mutate(topic, state -> {
            // 生产侧幂等：相同 producerKey 直接返回既有消息，且显式标记 duplicate。
            if (producerKey != null && !producerKey.isBlank()) {
                String existingId = state.getProducerKeyIndex().get(producerKey);
                if (existingId != null) {
                    QueueMessage existing = state.getMessages().stream()
                            .filter(m -> m.getMessageId().equals(existingId)).findFirst().orElseThrow();
                    log.info("生产去重命中 topic={} producerKey={} messageId={} offset={}",
                            topic, producerKey, existingId, existing.getOffset());
                    return new ProduceReceipt(existingId, existing.getOffset(), true);
                }
            }
            // 先回收已终结消息：容量按保留消息总数计，真实约束长期占用。
            compact(state);
            // 有界背压：容量按保留消息总数（含未终结与保留期内的终结消息）计。
            // BATCH 策略下单条生产与 REJECT 一致：立即拒绝，成批接收走 produceBatch。
            BackpressureStrategy strategy = state.getConfig().getBackpressureStrategy();
            if (state.getMessages().size() >= state.getConfig().getCapacity()
                    && (strategy == BackpressureStrategy.REJECT
                        || strategy == BackpressureStrategy.BATCH)) {
                throw new QueueException(ErrorCode.QUEUE_FULL,
                        "队列已满 topic=" + state.getName() + "，容量上限 "
                                + state.getConfig().getCapacity() + "，按 "
                                + strategy + " 策略拒绝单条生产");
            }
            if (!backend.awaitCapacity(topic, backpressureTimeoutMillis,
                    s -> s.getMessages().size() < s.getConfig().getCapacity())) {
                long waited = state.getConfig().getBackpressureTimeout().toMillis();
                throw new QueueException(ErrorCode.BACKPRESSURE_TIMEOUT,
                        "背压等待超时 topic=" + topic + "，超时阈值 " + waited + "ms，容量 "
                                + state.getConfig().getCapacity());
            }
            QueueMessage msg = new QueueMessage();
            msg.setMessageId(UUID.randomUUID().toString());
            msg.setTopic(topic);
            msg.setOffset(state.getNextOffset());
            msg.setBody(body);
            msg.setProducerKey(producerKey);
            msg.setEnqueueMillis(clock.millis());
            msg.setAttempt(0);
            msg.setState(MessageState.AVAILABLE);
            state.getMessages().add(msg);
            state.setNextOffset(state.getNextOffset() + 1);
            if (producerKey != null && !producerKey.isBlank()) {
                state.getProducerKeyIndex().put(producerKey, msg.getMessageId());
            }
            log.info("消息已生产 topic={} messageId={} offset={} producerKey={}",
                    topic, msg.getMessageId(), msg.getOffset(), producerKey);
            return new ProduceReceipt(msg.getMessageId(), msg.getOffset(), false);
        });
    }

    @Override
    public BatchProduceResult produceBatch(String token, String topic, List<BatchItem> items) {
        checkOpen();
        auth(token, topic, Permission.PRODUCE);
        if (items == null || items.isEmpty()) {
            throw new QueueException(ErrorCode.BAD_REQUEST, "批量生产不能为空");
        }
        int maxBatch = backend.readTopic(topic).getConfig().getMaxBatchSize();
        if (items.size() > maxBatch) {
            throw new QueueException(ErrorCode.BATCH_TOO_LARGE,
                    "批量大小超限 topic=" + topic + " size=" + items.size()
                            + " maxBatchSize=" + maxBatch);
        }
        return backend.mutate(topic, state -> {
            // 先回收终结消息再评估容量，与单条生产同一容量语义。
            compact(state);
            List<BatchProduceResult.Item> out = new ArrayList<>();
            int index = 0;
            for (BatchItem item : items) {
                String producerKey = item.getProducerKey();
                // 生产侧幂等：批内与历史同样去重，命中不算失败。
                if (producerKey != null && !producerKey.isBlank()) {
                    String existingId = state.getProducerKeyIndex().get(producerKey);
                    if (existingId != null) {
                        QueueMessage existing = state.getMessages().stream()
                                .filter(m -> m.getMessageId().equals(existingId))
                                .findFirst().orElseThrow();
                        out.add(BatchProduceResult.Item.duplicate(
                                index, existingId, existing.getOffset()));
                        index++;
                        continue;
                    }
                }
                // 成批接收：放得下的收下，放不下的逐条标记拒绝，不回退已接收部分。
                if (state.getMessages().size() >= state.getConfig().getCapacity()) {
                    out.add(BatchProduceResult.Item.rejected(index, ErrorCode.QUEUE_FULL,
                            "队列已满，该条未接收 topic=" + topic + " 容量 "
                                    + state.getConfig().getCapacity()));
                    index++;
                    continue;
                }
                QueueMessage msg = new QueueMessage();
                msg.setMessageId(UUID.randomUUID().toString());
                msg.setTopic(topic);
                msg.setOffset(state.getNextOffset());
                msg.setBody(item.getBody());
                msg.setProducerKey(producerKey);
                msg.setEnqueueMillis(clock.millis());
                msg.setAttempt(0);
                msg.setState(MessageState.AVAILABLE);
                state.getMessages().add(msg);
                state.setNextOffset(state.getNextOffset() + 1);
                if (producerKey != null && !producerKey.isBlank()) {
                    state.getProducerKeyIndex().put(producerKey, msg.getMessageId());
                }
                out.add(BatchProduceResult.Item.accepted(index, msg.getMessageId(),
                        msg.getOffset()));
                index++;
            }
            BatchProduceResult result = new BatchProduceResult(out);
            log.info("批量生产完成 topic={} total={} accepted={} rejected={}",
                    topic, items.size(), result.getAcceptedCount(), result.getRejectedCount());
            return result;
        });
    }

    @Override
    public BatchCommitResult commitBatch(String token, String topic, String group,
                                         List<String> deliveryIds) {
        checkOpen();
        auth(token, topic, Permission.CONSUME);
        if (deliveryIds == null || deliveryIds.isEmpty()) {
            throw new QueueException(ErrorCode.BAD_REQUEST, "批量提交不能为空");
        }
        int maxBatch = backend.readTopic(topic).getConfig().getMaxBatchSize();
        if (deliveryIds.size() > maxBatch) {
            throw new QueueException(ErrorCode.BATCH_TOO_LARGE,
                    "批量大小超限 topic=" + topic + " size=" + deliveryIds.size()
                            + " maxBatchSize=" + maxBatch);
        }
        // 整批在主题锁内完成：与并发生产/消费互斥，成功条目不回退，
        // 失败条目逐条记录稳定错误码；位点仍按连续水位推进。
        BatchCommitResult result = backend.mutate(topic, state -> {
            GroupState gs = requireGroup(state, group);
            List<BatchCommitResult.Item> out = new ArrayList<>();
            for (String deliveryId : deliveryIds) {
                try {
                    CommitResult cr = doCommit(state, gs, deliveryId);
                    out.add(BatchCommitResult.Item.ok(deliveryId, cr));
                } catch (QueueException ex) {
                    out.add(BatchCommitResult.Item.failed(deliveryId, ex.getCode(),
                            ex.getMessage()));
                }
            }
            compact(state);
            backend.signalCapacity(state.getName());
            return new BatchCommitResult(out);
        });
        long firstCommits = result.getItems().stream()
                .filter(i -> i.outcome() == CommitOutcome.COMMITTED).count();
        inflightCount.addAndGet((int) -firstCommits);
        log.info("批量提交完成 topic={} group={} total={} committed={} failed={}",
                topic, group, deliveryIds.size(), result.getCommittedCount(),
                result.getFailedCount());
        return result;
    }

    @Override
    public int reclaimTerminated(String topic) {
        checkOpen();
        int reclaimed = backend.mutate(topic, this::compact);
        if (reclaimed > 0) {
            backend.signalCapacity(topic);
        }
        return reclaimed;
    }

    @Override
    public Delivery poll(String token, String topic, String group) {
        checkOpen();
        auth(token, topic, Permission.CONSUME);
        Delivery delivery = backend.mutate(topic, state ->
                dispatch(state, requireGroup(state, group)));
        if (delivery != null) {
            inflightCount.incrementAndGet();
        }
        return delivery;
    }

    private GroupState requireGroup(TopicState state, String group) {
        GroupState gs = state.getGroups().get(group);
        if (gs == null) {
            throw new QueueException(ErrorCode.GROUP_NOT_FOUND,
                    "消费者组不存在: " + group + " (topic=" + state.getName() + ")");
        }
        return gs;
    }

    /**
     * 在主题状态上挑选一条对该组可投递的消息并生成投递。
     * 可见性：AVAILABLE 且 availableAfter &lt;= now；
     * 位点顺序：按 offset 升序。同一组同一消息任一时刻至多一个生效投递。
     * 投递原因按以下优先级可区分：REPLAY（位点重放）&gt;
     * VISIBILITY_TIMEOUT（超时回收）&gt; RETRY（显式 nack 退避到期）&gt; FIRST。
     */
    private Delivery dispatch(TopicState state, GroupState group) {
        long now = clock.millis();
        for (QueueMessage msg : state.getMessages()) {
            if (msg.getState() == MessageState.COMMITTED || msg.getState() == MessageState.DEAD) {
                continue;
            }
            GroupMessageState gs = state.stateFor(group.getName(), msg.getMessageId());
            if (gs.getState() != GroupDeliveryState.AVAILABLE) {
                continue;
            }
            if (gs.getAvailableAfterMillis() > now) {
                continue;
            }
            DeliveryReason reason;
            if (gs.isReplayPending()) {
                reason = DeliveryReason.REPLAY;
            } else if (gs.isTimeoutReclaimed()) {
                reason = DeliveryReason.VISIBILITY_TIMEOUT;
            } else if (gs.getAttempts() > 0) {
                reason = DeliveryReason.RETRY;
            } else {
                reason = DeliveryReason.FIRST;
            }
            String deliveryId = UUID.randomUUID().toString();
            long visibleUntil = now + state.getConfig().getVisibilityTimeout().toMillis();
            gs.setState(GroupDeliveryState.INFLIGHT);
            gs.setCurrentDeliveryId(deliveryId);
            gs.setLastDeliveryId(deliveryId);
            gs.setAttempts(gs.getAttempts() + 1);
            gs.setVisibleUntilMillis(visibleUntil);
            gs.setLastDispatchedMillis(now);
            gs.setTimeoutReclaimed(false);
            gs.setReplayPending(false);
            msg.setState(MessageState.INFLIGHT);

            Delivery d = new Delivery();
            d.setDeliveryId(deliveryId);
            d.setMessageId(msg.getMessageId());
            d.setTopic(state.getName());
            d.setGroup(group.getName());
            d.setOffset(msg.getOffset());
            d.setBody(msg.getBody());
            d.setAttempt(gs.getAttempts());
            d.setReason(reason);
            d.setVisibleUntilMillis(visibleUntil);
            d.setDeliveredAtMillis(now);
            log.info("消息投递 topic={} group={} messageId={} offset={} attempt={} reason={} deliveryId={}",
                    state.getName(), group.getName(), msg.getMessageId(), msg.getOffset(),
                    gs.getAttempts(), reason, deliveryId);
            return d;
        }
        return null;
    }

    @Override
    public CommitResult commit(String token, String topic, String group, String deliveryId) {
        checkOpen();
        auth(token, topic, Permission.CONSUME);
        CommitResult result = backend.mutate(topic, state -> {
            CommitResult cr = doCommit(state, requireGroup(state, group), deliveryId);
            // 提交可能终结消息：先回收再唤醒，等待中的生产者才能看到容量释放。
            compact(state);
            backend.signalCapacity(state.getName());
            return cr;
        });
        inflightCount.decrementAndGet();
        return result;
    }

    /** 提交核心：校验投递归属与有效性，拒绝陈旧投递，单调推进位点。 */
    private CommitResult doCommit(TopicState state, GroupState group, String deliveryId) {
        DeliveryRef ref = resolveDelivery(state, group, deliveryId);
        QueueMessage msg = ref.message;
        GroupMessageState gs = ref.groupMessage;
        if (gs.getState() == GroupDeliveryState.COMMITTED) {
            // 仅认该消息最近一次已完成的投递标识：合法重复提交幂等成功。
            if (deliveryId.equals(gs.getLastDeliveryId())) {
                log.info("重复提交（幂等成功）topic={} group={} messageId={} offset={} deliveryId={}",
                        state.getName(), group.getName(), msg.getMessageId(), msg.getOffset(), deliveryId);
                return new CommitResult(CommitOutcome.ALREADY_COMMITTED, msg.getMessageId(), msg.getOffset());
            }
            throw new QueueException(ErrorCode.DELIVERY_STALE,
                    "提交被拒绝：该消息已由更新的投递提交（DUPLICATE_DELIVERY）topic="
                            + state.getName() + " group=" + group.getName()
                            + " messageId=" + msg.getMessageId() + " offset=" + msg.getOffset());
        }
        if (gs.getState() != GroupDeliveryState.INFLIGHT
                || !deliveryId.equals(gs.getCurrentDeliveryId())) {
            throw new QueueException(ErrorCode.DELIVERY_STALE,
                    "提交被拒绝：投递已失效（可能已超时重投）topic=" + state.getName()
                            + " group=" + group.getName() + " messageId=" + msg.getMessageId()
                            + " offset=" + msg.getOffset());
        }
        gs.setState(GroupDeliveryState.COMMITTED);
        gs.setCurrentDeliveryId(null);
        // 连续水位推进：乱序提交时位点不得跳过未完成的更早消息。
        advanceGroupOffset(state, group);
        idempotencyGuard.commit(msg.getMessageId());
        // 若所有组都已提交，则主题级消息状态终结；有一个组仍待消费则保持在途。
        boolean allCommitted = state.getGroups().values().stream()
                .allMatch(g -> {
                    GroupMessageState s = g.getMessages().get(msg.getMessageId());
                    return s != null && s.getState() == GroupDeliveryState.COMMITTED;
                });
        if (allCommitted) {
            msg.setState(MessageState.COMMITTED);
            msg.setTerminatedAtMillis(clock.millis());
        }
        log.info("消息提交成功 topic={} group={} messageId={} offset={} committedOffset={} attempt={}",
                state.getName(), group.getName(), msg.getMessageId(), msg.getOffset(),
                group.getCommittedOffset(), gs.getAttempts());
        return new CommitResult(CommitOutcome.COMMITTED, msg.getMessageId(), msg.getOffset());
    }

    private record DeliveryRef(QueueMessage message, GroupMessageState groupMessage) {
    }

    /** 按位点定位消息；offset &lt; baseOffset 表示已被回收，返回 null。 */
    private static QueueMessage messageAt(TopicState state, long offset) {
        long idx = offset - state.getBaseOffset();
        if (idx < 0 || idx >= state.getMessages().size()) {
            return null;
        }
        QueueMessage m = state.getMessages().get((int) idx);
        return m.getOffset() == offset ? m : null;
    }

    /**
     * 连续水位推进：从已提交位点之后逐位点检查，仅当该位点消息在本组已终结
     * （提交或死信，组状态均为 COMMITTED）才前进。乱序提交时水位停在最早
     * 未完成消息之前，重启恢复后从该消息继续投递，不跳过、不重复。
     */
    private static void advanceGroupOffset(TopicState state, GroupState group) {
        long next = group.getCommittedOffset() + 1;
        if (next < state.getBaseOffset()) {
            // 已回收区间必然对本组终结，水位先对齐保留边界（保持单调不倒退）。
            group.setCommittedOffset(state.getBaseOffset() - 1);
            next = state.getBaseOffset();
        }
        while (true) {
            QueueMessage m = messageAt(state, next);
            if (m == null) {
                return; // 该位点尚未生产
            }
            GroupMessageState gs = group.getMessages().get(m.getMessageId());
            if (gs == null || gs.getState() != GroupDeliveryState.COMMITTED) {
                return; // 该位点未完成，水位停在其前
            }
            group.setCommittedOffset(next);
            next++;
        }
    }

    /**
     * 定位投递：先在本组找 current/last 匹配；若该 deliveryId 实际属于其它组，
     * 以 CROSS_GROUP_COMMIT_REJECTED 明确拒绝；都找不到则 DELIVERY_NOT_FOUND。
     */
    private DeliveryRef resolveDelivery(TopicState state, GroupState group, String deliveryId) {
        if (deliveryId == null || deliveryId.isBlank()) {
            throw new QueueException(ErrorCode.DELIVERY_NOT_FOUND, "deliveryId 不能为空");
        }
        for (QueueMessage msg : state.getMessages()) {
            GroupMessageState gs = group.getMessages().get(msg.getMessageId());
            if (gs == null) {
                continue;
            }
            if (deliveryId.equals(gs.getCurrentDeliveryId())
                    || deliveryId.equals(gs.getLastDeliveryId())) {
                return new DeliveryRef(msg, gs);
            }
        }
        for (GroupState other : state.getGroups().values()) {
            if (other.getName().equals(group.getName())) {
                continue;
            }
            for (GroupMessageState otherState : other.getMessages().values()) {
                if (deliveryId.equals(otherState.getCurrentDeliveryId())
                        || deliveryId.equals(otherState.getLastDeliveryId())) {
                    throw new QueueException(ErrorCode.CROSS_GROUP_COMMIT_REJECTED,
                            "提交被拒绝：投递属于其它消费者组（禁止跨组提交）topic="
                                    + state.getName() + " 本组=" + group.getName()
                                    + " 归属组=" + other.getName());
                }
            }
        }
        throw new QueueException(ErrorCode.DELIVERY_NOT_FOUND,
                "投递不存在或不属于该消费者组（拒绝跨组提交）topic=" + state.getName()
                        + " group=" + group.getName() + " deliveryId=" + deliveryId);
    }

    @Override
    public NackResult nack(String token, String topic, String group, String deliveryId,
                           String error, boolean retryable) {
        checkOpen();
        auth(token, topic, Permission.CONSUME);
        NackResult result = backend.mutate(topic, state -> {
            NackResult nr = doNack(state, requireGroup(state, group), deliveryId, error, retryable);
            compact(state);
            backend.signalCapacity(state.getName());
            return nr;
        });
        inflightCount.decrementAndGet();
        return result;
    }

    private NackResult doNack(TopicState state, GroupState group, String deliveryId,
                              String error, boolean retryable) {
        DeliveryRef ref = resolveDelivery(state, group, deliveryId);
        QueueMessage msg = ref.message;
        GroupMessageState gs = ref.groupMessage;
        if (gs.getState() != GroupDeliveryState.INFLIGHT
                || !deliveryId.equals(gs.getCurrentDeliveryId())) {
            throw new QueueException(ErrorCode.DELIVERY_STALE,
                    "否定确认被拒绝：投递已失效（已超时重投或已提交）messageId="
                            + msg.getMessageId());
        }
        int attempts = gs.getAttempts();
        int maxAttempts = state.getConfig().getBackoff().getMaxAttempts();
        boolean exhausted = attempts >= maxAttempts;
        if (!retryable || exhausted) {
            return moveToDeadLetter(state, group, msg, gs, attempts,
                    !retryable ? DeadLetterRecord.Cause.NON_RETRYABLE
                            : DeadLetterRecord.Cause.RETRIES_EXHAUSTED,
                    error);
        }
        long delayMillis = state.getConfig().getBackoff().delayForAttempt(attempts).toMillis();
        long availableAfter = clock.millis() + delayMillis;
        gs.setState(GroupDeliveryState.AVAILABLE);
        gs.setCurrentDeliveryId(null);
        gs.setVisibleUntilMillis(0);
        gs.setAvailableAfterMillis(availableAfter);
        gs.setTimeoutReclaimed(false);
        msg.setState(MessageState.RETRY_WAIT);
        log.info("消息安排重试 topic={} group={} messageId={} offset={} attempt={} retryAfterMs={}",
                state.getName(), group.getName(), msg.getMessageId(), msg.getOffset(),
                attempts, delayMillis);
        return new NackResult(NackResult.Status.RETRY_SCHEDULED, attempts, delayMillis, null);
    }

    private NackResult moveToDeadLetter(TopicState state, GroupState group, QueueMessage msg,
                                        GroupMessageState gs, int attempts,
                                        DeadLetterRecord.Cause cause, String error) {
        gs.setState(GroupDeliveryState.COMMITTED); // 该组终结，不再投递
        gs.setCurrentDeliveryId(null);
        boolean anyOtherActive = state.getGroups().entrySet().stream()
                .anyMatch(e -> !e.getKey().equals(group.getName())
                        && isActive(e.getValue().getMessages().get(msg.getMessageId())));
        if (!anyOtherActive) {
            msg.setState(MessageState.DEAD);
            msg.setTerminatedAtMillis(clock.millis());
        }
        DeadLetterRecord record = new DeadLetterRecord();
        record.setMessageId(msg.getMessageId());
        record.setTopic(state.getName());
        record.setOffset(msg.getOffset());
        record.setBody(msg.getBody());
        record.setAttempts(attempts);
        record.setCause(cause);
        record.setDeadAtMillis(clock.millis());
        record.setLastError(error);
        state.getDeadLetters().add(record);
        log.warn("消息进入死信 topic={} group={} messageId={} offset={} attempts={} cause={}",
                state.getName(), group.getName(), msg.getMessageId(), msg.getOffset(),
                attempts, cause);
        return new NackResult(NackResult.Status.DEAD_LETTER, attempts, 0, record);
    }

    private boolean isActive(GroupMessageState s) {
        return s != null && s.getState() != GroupDeliveryState.COMMITTED;
    }

    @Override
    public <R> ProcessResult<R> process(String token, String topic, String group,
                                        Function<Delivery, R> handler) {
        checkOpen();
        Delivery delivery = poll(token, topic, group);
        if (delivery == null) {
            return new ProcessResult<>(ProcessResult.Status.NO_MESSAGE, null, null, null, 0, null);
        }
        String key = delivery.getMessageId();
        // 已生效（历史重复投递）：不再执行业务，提交结果可区分。
        if (idempotencyGuard.isApplied(key)) {
            // 重复投递不重复执行业务，直接按幂等提交终结本次在途投递。
            CommitResult cr = commit(token, topic, group, delivery.getDeliveryId());
            log.info("重复投递短路（业务不重复生效）topic={} group={} messageId={} offset={} attempt={} reason={}",
                    topic, group, key, delivery.getOffset(), delivery.getAttempt(), delivery.getReason());
            return new ProcessResult<>(ProcessResult.Status.ALREADY_PROCESSED, null, cr,
                    key, delivery.getAttempt(), delivery.getReason());
        }
        if (!idempotencyGuard.begin(key)) {
            // 另一消费者正在处理同一条消息（竞态）：交还本次投递，不生效。
            backend.mutate(topic, state -> releaseInflight(state, group, delivery));
            inflightCount.decrementAndGet();
            throw new QueueException(ErrorCode.DELIVERY_STALE,
                    "消息正在被另一消费者处理，本次投递被拒绝 messageId=" + key);
        }
        R value;
        try {
            value = handler.apply(delivery);
        } catch (RuntimeException ex) {
            idempotencyGuard.release(key);
            try {
                nack(token, topic, group, delivery.getDeliveryId(),
                        String.valueOf(ex.getMessage()), true);
            } catch (RuntimeException suppressed) {
                ex.addSuppressed(suppressed);
            }
            log.warn("业务处理失败，已安排重试或死信 messageId={} attempt={}", key,
                    delivery.getAttempt(), ex);
            return new ProcessResult<>(ProcessResult.Status.FAILED, null, null,
                    key, delivery.getAttempt(), delivery.getReason());
        }
        // 业务已生效：提交若因陈旧投递失败，不能重复业务，直接抛出由调用方感知；
        // 幂等提交返回 ALREADY_COMMITTED 同样视为成功。
        CommitResult cr = commit(token, topic, group, delivery.getDeliveryId());
        return new ProcessResult<>(ProcessResult.Status.APPLIED, value, cr,
                key, delivery.getAttempt(), delivery.getReason());
    }

    /** 竞态放弃：把刚分发的在途状态恢复为可投递（保留其 attempt 已增长的事实影响极小，
     * 此处恢复 attempt 以不计费这次未生效投递）。 */
    private Void releaseInflight(TopicState state, String groupName, Delivery delivery) {
        GroupState group = requireGroup(state, groupName);
        GroupMessageState gs = group.getMessages().get(delivery.getMessageId());
        if (gs != null && delivery.getDeliveryId().equals(gs.getCurrentDeliveryId())) {
            gs.setState(GroupDeliveryState.AVAILABLE);
            gs.setCurrentDeliveryId(null);
            gs.setVisibleUntilMillis(0);
            gs.setAttempts(Math.max(0, gs.getAttempts() - 1));
            QueueMessage msg = state.getMessages().stream()
                    .filter(m -> m.getMessageId().equals(delivery.getMessageId())).findFirst().orElse(null);
            if (msg != null) {
                msg.setState(MessageState.AVAILABLE);
            }
        }
        return null;
    }

    @Override
    public OffsetInfo offsetOf(String token, String topic, String group) {
        checkOpen();
        auth(token, topic, Permission.CONSUME);
        return backend.mutate(topic, state -> buildOffsetInfo(state, requireGroup(state, group)));
    }

    private OffsetInfo buildOffsetInfo(TopicState state, GroupState group) {
        int inflight = 0;
        for (GroupMessageState gs : group.getMessages().values()) {
            if (gs.getState() == GroupDeliveryState.INFLIGHT) {
                inflight++;
            }
        }
        return new OffsetInfo(state.getName(), group.getName(),
                group.getCommittedOffset(), group.getCommittedOffset() + 1, inflight);
    }

    @Override
    public OffsetInfo replay(String token, String topic, String group, long targetOffset) {
        checkOpen();
        auth(token, topic, Permission.ADMIN);
        if (targetOffset < -1) {
            throw new QueueException(ErrorCode.BAD_REQUEST,
                    "重放位点非法（允许 -1 表示从头开始）: " + targetOffset);
        }
        return backend.mutate(topic, state -> {
            GroupState gs = requireGroup(state, group);
            if (targetOffset > gs.getCommittedOffset()) {
                throw new QueueException(ErrorCode.BAD_REQUEST,
                        "重放目标位点超过当前已提交位点: target=" + targetOffset
                                + " committed=" + gs.getCommittedOffset());
            }
            if (targetOffset == gs.getCommittedOffset()) {
                throw new QueueException(ErrorCode.OFFSET_ROLLBACK_REJECTED,
                        "重放位点必须早于当前位点（无操作被拒绝）target=" + targetOffset);
            }
            if (targetOffset < state.getBaseOffset() - 1) {
                // 重放区间起点已落出保留范围：明确拒绝，绝不悄悄跳过已回收历史。
                throw new QueueException(ErrorCode.OFFSET_COMPACTED,
                        "重放目标位点已被回收（超出保留边界）topic=" + state.getName()
                                + " group=" + group + " target=" + targetOffset
                                + " 最早可重放位点=" + (state.getBaseOffset() - 1));
            }
            // 将位点大于 targetOffset 的本组已提交消息全部重置为可投递。
            // 乱序提交下已提交消息可能高于水位，必须一并覆盖，保证重放区间完整。
            for (QueueMessage msg : state.getMessages()) {
                if (msg.getOffset() > targetOffset) {
                    GroupMessageState gms = gs.getMessages().get(msg.getMessageId());
                    if (gms != null && gms.getState() == GroupDeliveryState.COMMITTED) {
                        gms.setState(GroupDeliveryState.AVAILABLE);
                        gms.setCurrentDeliveryId(null);
                        gms.setLastDeliveryId(null);
                        gms.setVisibleUntilMillis(0);
                        gms.setAvailableAfterMillis(clock.millis());
                        gms.setReplayPending(true);
                        gms.setTimeoutReclaimed(false);
                        if (msg.getState() == MessageState.COMMITTED
                                || msg.getState() == MessageState.DEAD) {
                            msg.setState(MessageState.AVAILABLE);
                        }
                    }
                }
            }
            gs.setCommittedOffset(targetOffset);
            log.warn("位点重放 topic={} group={} newCommittedOffset={}",
                    state.getName(), group, targetOffset);
            return buildOffsetInfo(state, gs);
        });
    }

    @Override
    public List<DeadLetterRecord> deadLetters(String token, String topic) {
        checkOpen();
        auth(token, topic, Permission.CONSUME);
        return List.copyOf(backend.readTopic(topic).getDeadLetters());
    }

    @Override
    public int reclaimExpired(String topic) {
        checkOpen();
        int[] reclaimed = {0};
        backend.mutate(topic, state -> {
            long now = clock.millis();
            for (GroupState group : state.getGroups().values()) {
                for (Map.Entry<String, GroupMessageState> e : group.getMessages().entrySet()) {
                    GroupMessageState gs = e.getValue();
                    if (gs.getState() != GroupDeliveryState.INFLIGHT) {
                        continue;
                    }
                    if (gs.getVisibleUntilMillis() > now) {
                        continue;
                    }
                    // 可见性超时：未提交的在途消息重新可投，绝不永久丢失。
                    gs.setState(GroupDeliveryState.AVAILABLE);
                    gs.setCurrentDeliveryId(null);
                    gs.setVisibleUntilMillis(0);
                    gs.setAvailableAfterMillis(now);
                    gs.setTimeoutReclaimed(true);
                    reclaimed[0]++;
                    QueueMessage msg = state.getMessages().stream()
                            .filter(m -> m.getMessageId().equals(e.getKey())).findFirst().orElse(null);
                    if (msg != null && msg.getState() == MessageState.INFLIGHT) {
                        msg.setState(MessageState.AVAILABLE);
                    }
                    log.warn("可见性超时回收 topic={} group={} messageId={} attempt={} nextReason=VISIBILITY_TIMEOUT",
                            state.getName(), group.getName(), e.getKey(), gs.getAttempts());
                }
            }
            return null;
        });
        inflightCount.addAndGet(-reclaimed[0]);
        return reclaimed[0];
    }

    @Override
    public void close(Duration gracefulWait) {
        if (closed) {
            return;
        }
        closed = true;
        // 使用真实墙钟等待，不能依赖可注入（可能被测试冻结）的业务时钟。
        long waitMillis = gracefulWait == null ? 0 : gracefulWait.toMillis();
        long deadline = System.currentTimeMillis() + waitMillis;
        // 等待在途消息完成提交/失败，避免应用关闭导致消息丢失。
        // 在途数以持久化状态为准（重启后内存计数器不参与判定）。
        while (totalInflightFromState() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        // 仍未完成的在途消息：安全交还为可投递（重启后由其它消费者继续）。
        for (String topic : backend.listTopics()) {
            int returned = 0;
            try {
                returned = backend.mutate(topic, state -> {
                    int[] n = {0};
                    long now = clock.millis();
                    for (GroupState group : state.getGroups().values()) {
                        for (Map.Entry<String, GroupMessageState> e : group.getMessages().entrySet()) {
                            GroupMessageState gs = e.getValue();
                            if (gs.getState() == GroupDeliveryState.INFLIGHT) {
                                gs.setState(GroupDeliveryState.AVAILABLE);
                                gs.setCurrentDeliveryId(null);
                                gs.setVisibleUntilMillis(0);
                                gs.setAvailableAfterMillis(now);
                                gs.setTimeoutReclaimed(true);
                                n[0]++;
                            }
                        }
                    }
                    return n[0];
                });
            } catch (RuntimeException ex) {
                log.error("关闭时交还在途消息失败 topic={}", topic, ex);
            }
            if (returned > 0) {
                log.warn("应用关闭，安全交还在途消息 topic={} count={}", topic, returned);
            }
        }
        // 注意：不关闭后端。后端是可复用的存储资源，文件后端的每次变更都已在
        // mutate 锁内即时原子落盘，无额外缓冲需要 flush；运行时关闭只拒绝新操作。
        log.info("运行时已关闭");
    }

    /** 按持久化状态统计当前全部在途消息数（不依赖易失的内存计数器）。 */
    private int totalInflightFromState() {
        int total = 0;
        for (String topic : backend.listTopics()) {
            TopicState state = backend.readTopic(topic);
            for (GroupState group : state.getGroups().values()) {
                for (GroupMessageState gs : group.getMessages().values()) {
                    if (gs.getState() == GroupDeliveryState.INFLIGHT) {
                        total++;
                    }
                }
            }
        }
        return total;
    }
}
