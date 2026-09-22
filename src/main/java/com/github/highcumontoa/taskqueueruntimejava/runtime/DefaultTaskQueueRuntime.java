package com.github.highcumontoa.taskqueueruntimejava.runtime;

import com.github.highcumontoa.taskqueueruntimejava.backend.GroupMessageState;
import com.github.highcumontoa.taskqueueruntimejava.backend.GroupState;
import com.github.highcumontoa.taskqueueruntimejava.backend.QueueBackend;
import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.error.QueueException;
import com.github.highcumontoa.taskqueueruntimejava.model.BackpressureStrategy;
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

    private static long pendingCount(TopicState state) {
        return state.getMessages().stream()
                .filter(m -> m.getState() != MessageState.COMMITTED
                        && m.getState() != MessageState.DEAD)
                .count();
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
            // 有界背压：容量按未终结消息计，任何情况下都不允许无界增长。
            if (pendingCount(state) >= state.getConfig().getCapacity()
                    && state.getConfig().getBackpressureStrategy() == BackpressureStrategy.REJECT) {
                throw new QueueException(ErrorCode.QUEUE_FULL,
                        "队列已满 topic=" + state.getName() + "，容量上限 "
                                + state.getConfig().getCapacity() + "，按 REJECT 策略拒绝生产");
            }
            if (!backend.awaitCapacity(topic, backpressureTimeoutMillis,
                    s -> pendingCount(s) < s.getConfig().getCapacity())) {
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
            // 容量被释放，唤醒背压等待中的生产者。
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
        // 位点只能单调推进；显式回退由 replay 负责。
        if (msg.getOffset() > group.getCommittedOffset()) {
            group.setCommittedOffset(msg.getOffset());
        }
        idempotencyGuard.commit(msg.getMessageId());
        // 若所有组都已提交，则主题级消息状态终结；有一个组仍待消费则保持在途。
        boolean allCommitted = state.getGroups().values().stream()
                .allMatch(g -> {
                    GroupMessageState s = g.getMessages().get(msg.getMessageId());
                    return s != null && s.getState() == GroupDeliveryState.COMMITTED;
                });
        if (allCommitted) {
            msg.setState(MessageState.COMMITTED);
        }
        log.info("消息提交成功 topic={} group={} messageId={} offset={} committedOffset={} attempt={}",
                state.getName(), group.getName(), msg.getMessageId(), msg.getOffset(),
                group.getCommittedOffset(), gs.getAttempts());
        return new CommitResult(CommitOutcome.COMMITTED, msg.getMessageId(), msg.getOffset());
    }

    private record DeliveryRef(QueueMessage message, GroupMessageState groupMessage) {
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
            // 将 (targetOffset, committed] 区间内本组已提交消息重置为可投递。
            for (QueueMessage msg : state.getMessages()) {
                if (msg.getOffset() > targetOffset && msg.getOffset() <= gs.getCommittedOffset()) {
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
