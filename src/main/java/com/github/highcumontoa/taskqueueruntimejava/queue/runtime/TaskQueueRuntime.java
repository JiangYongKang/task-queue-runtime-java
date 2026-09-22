package com.github.highcumontoa.taskqueueruntimejava.queue.runtime;

import com.github.highcumontoa.taskqueueruntimejava.queue.config.QueueRuntimeProperties;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.AuthorizationException;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.BackendUnavailableException;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.InvalidCredentialException;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.OffsetRejectedException;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.ProduceTimeoutException;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.QueueFullException;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.QueueRuntimeException;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.RuntimeClosedException;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.CommitOutcome;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.CommitReceipt;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.DeadLetterReason;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.DeadLetterRecord;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.DeliveryReason;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueOutcome;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueReceipt;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.EnqueueRequest;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Lease;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.MessageState;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.NackOutcome;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.NackReceipt;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Permission;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Principal;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.QueueEvent;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.QueueMessage;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.ReceiveResult;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.RetryPolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.TopicSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.storage.QueueStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * 可嵌入的本地任务队列运行时。
 *
 * <p>语义保证：</p>
 * <ul>
 *   <li>同一（主题, 组）下，一条消息任一时刻最多一个在途租约；</li>
 *   <li>至少一次投递 + 业务幂等键去重，重复投递与合法重试通过 {@link DeliveryReason} 与
 *       回执 outcome 可区分；</li>
 *   <li>nack 按指数退避重试，次数有上限，超限/不可重试进入可查询死信；</li>
 *   <li>位点与状态可持久化（{@code LocalFileStorage}），重启不跳变；回退/跨组提交被拒绝；</li>
 *   <li>主题深度有界，背压策略 REJECT/DELAY/BATCH；</li>
 *   <li>关闭时回收全部在途租约，消息可被重新投递。</li>
 * </ul>
 */
public final class TaskQueueRuntime implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TaskQueueRuntime.class);
    private static final SecureRandom RANDOM = new SecureRandom();
    public static final String DEFAULT_ADMIN_TOKEN = "local-admin-token";

    private final QueueStorage storage;
    private final QueueRuntimeProperties properties;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final List<Consumer<QueueEvent>> listeners = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private Thread reaperThread;

    public TaskQueueRuntime(QueueStorage storage, QueueRuntimeProperties properties) {
        this.storage = storage;
        this.properties = properties;
        Principal admin = storage.principal(DEFAULT_ADMIN_TOKEN);
        if (admin == null) {
            storage.registerPrincipal(new Principal(DEFAULT_ADMIN_TOKEN,
                    Set.of(Permission.ADMIN), Set.of("*")));
        }
        startReaper();
    }

    // ================================================================ 管理面

    public void createTopic(String token, String topic, TopicSettings settings) {
        lock.lock();
        try {
            checkOpen();
            require(token, Permission.ADMIN, null);
            TopicSettings effective = settings != null ? settings
                    : new TopicSettings(properties.defaultTopicMaxDepth(),
                            properties.defaultBackpressurePolicy());
            storage.createTopic(topic, effective);
            log.info("topic created topic={} maxDepth={} backpressure={}",
                    topic, effective.maxDepth(), effective.backpressurePolicy());
        } finally {
            lock.unlock();
        }
    }

    public void createGroup(String token, String topic, String group, GroupSettings settings) {
        lock.lock();
        try {
            checkOpen();
            require(token, Permission.ADMIN, null);
            requireTopicExists(topic);
            GroupSettings effective = settings != null ? settings
                    : new GroupSettings(properties.defaultVisibilityTimeoutMillis(),
                            GroupSettings.defaults().retryPolicy());
            storage.createGroup(topic, group, effective);
            log.info("group created topic={} group={} visibilityTimeout={} retryPolicy={}",
                    topic, group, effective.visibilityTimeoutMillis(), effective.retryPolicy());
        } finally {
            lock.unlock();
        }
    }

    public Principal issueCredential(String token, Set<Permission> permissions, Set<String> grantedTopics) {
        lock.lock();
        try {
            checkOpen();
            require(token, Permission.ADMIN, null);
            byte[] bytes = new byte[24];
            RANDOM.nextBytes(bytes);
            String newToken = "qt_" + HexFormat.of().formatHex(bytes);
            Principal principal = new Principal(newToken, permissions, grantedTopics);
            storage.registerPrincipal(principal);
            log.info("credential issued permissions={} topics={}", permissions, grantedTopics);
            return principal;
        } finally {
            lock.unlock();
        }
    }

    public long committedOffset(String token, String topic, String group) {
        lock.lock();
        try {
            checkOpen();
            require(token, Permission.CONSUME, topic);
            return storage.loadGroupState(topic, group).committedOffset();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 管理员将位点重置（仅允许回退重放；前进会被拒绝）。
     * 重置将清除目标位点起的全部终态与在途状态，下一次 receive 按 REPLAY 原因投递。
     */
    public long resetOffset(String adminToken, String topic, String group, long target) {
        lock.lock();
        try {
            checkOpen();
            require(adminToken, Permission.ADMIN, topic);
            MutableGroupState s = MutableGroupState.load(storage, topic, group);
            long logStart = storage.logStartOffset(topic);
            long end = storage.nextOffset(topic);
            if (target < logStart || target > end) {
                throw new OffsetRejectedException(ErrorCode.OFFSET_OUT_OF_RANGE,
                        explain(topic, group, "reset target " + target + " out of range ["
                                + logStart + "," + end + "]"));
            }
            if (target >= s.committedOffset) {
                throw new OffsetRejectedException(ErrorCode.ILLEGAL_RESET_TARGET,
                        explain(topic, group, "reset target " + target
                                + " must be strictly before committedOffset " + s.committedOffset
                                + "; forward reset is not allowed"));
            }
            s.attempts.keySet().removeIf(o -> o >= target);
            s.availableAt.keySet().removeIf(o -> o >= target);
            s.idempotencyKeys.keySet().removeIf(o -> o >= target);
            s.pendingReasons.keySet().removeIf(o -> o >= target);
            s.leases.keySet().removeIf(o -> o >= target);
            s.ackedOffsets.removeIf(o -> o >= target);
            // 已处理幂等键也必须撤销，否则重放会被判为重复投递
            s.processedKeys.values().removeIf(firstOffset -> firstOffset >= target);
            s.committedTokens.values().removeIf(o -> o >= target);
            s.deadLetters.removeIf(d -> d.offset() >= target);
            s.committedOffset = target;
            s.nextOffset = Math.max(s.nextOffset, target);
            // 将重置范围内所有位点标记为 REPLAY，使重放投递与首次/重试可区分
            long topicEndForReset = storage.nextOffset(topic);
            for (long o = target; o < topicEndForReset; o++) {
                s.pendingReasons.put(o, DeliveryReason.REPLAY);
                s.availableAt.put(o, 0L);
            }
            s.save(storage);
            changed.signalAll();
            emit(QueueEvent.Kind.OFFSET_RESET, topic, group, target, null, 0,
                    "offset reset for replay");
            log.warn("offset reset topic={} group={} target={}", topic, group, target);
            return target;
        } finally {
            lock.unlock();
        }
    }

    public List<DeadLetterRecord> deadLetters(String token, String topic, String group) {
        lock.lock();
        try {
            checkOpen();
            require(token, Permission.CONSUME, topic);
            return List.copyOf(storage.loadGroupState(topic, group).deadLetters());
        } finally {
            lock.unlock();
        }
    }

    /** 查询某分组对某位点的当前状态（可解释性辅助 API）。 */
    public MessageState messageState(String token, String topic, String group, long offset) {
        lock.lock();
        try {
            checkOpen();
            require(token, Permission.CONSUME, topic);
            MutableGroupState s = MutableGroupState.load(storage, topic, group);
            if (s.leases.containsKey(offset)) {
                return MessageState.IN_FLIGHT;
            }
            if (s.deadLetters.stream().anyMatch(d -> d.offset() == offset)) {
                return MessageState.DEAD;
            }
            if (offset < s.committedOffset || s.ackedOffsets.contains(offset)) {
                return MessageState.COMMITTED;
            }
            return MessageState.READY;
        } finally {
            lock.unlock();
        }
    }

    public void addEventListener(Consumer<QueueEvent> listener) {
        listeners.add(listener);
    }

    // ================================================================ 生产侧

    public EnqueueReceipt produce(String token, EnqueueRequest request) {
        return produce(token, request, properties.defaultProduceWaitMillis());
    }

    public EnqueueReceipt produce(String token, EnqueueRequest request, long waitMillis) {
        lock.lock();
        try {
            checkOpen();
            require(token, Permission.PRODUCE, request.topic());
            return doProduce(request, waitMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueueRuntimeException(ErrorCode.PRODUCE_TIMEOUT,
                    "produce interrupted for topic " + request.topic(), e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 批量生产（BATCH 背压语义的显式入口）：将多个载荷合并为一条消息。
     * 数量超过 {@code maxBatch} 时以 {@link ErrorCode#BATCH_REJECTED} 稳定拒绝，保证有界。
     */
    public EnqueueReceipt produceBatch(String token, String topic, List<byte[]> payloads,
                                       int maxBatch, long waitMillis) {
        lock.lock();
        try {
            checkOpen();
            require(token, Permission.PRODUCE, topic);
            if (payloads == null || payloads.isEmpty()) {
                throw new IllegalArgumentException("batch payloads must not be empty");
            }
            if (payloads.size() > maxBatch) {
                throw new QueueFullException(ErrorCode.BATCH_REJECTED,
                        "batch size " + payloads.size() + " exceeds maxBatch " + maxBatch
                                + " for topic '" + topic + "'");
            }
            byte[] joined = joinPayloads(payloads);
            EnqueueRequest request = new EnqueueRequest(topic, joined, "application/x-batch",
                    Map.of("x-batch-size", Integer.toString(payloads.size())), null);
            EnqueueReceipt receipt = doProduce(request, waitMillis);
            return new EnqueueReceipt(topic, receipt.offset(), receipt.messageId(),
                    EnqueueOutcome.BATCHED);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueueRuntimeException(ErrorCode.PRODUCE_TIMEOUT,
                    "batch produce interrupted for topic " + topic, e);
        } finally {
            lock.unlock();
        }
    }

    private EnqueueReceipt doProduce(EnqueueRequest request, long waitMillis) throws InterruptedException {
        String topic = request.topic();
        TopicSettings settings = storage.topicSettings(topic);

        Long existing = storage.producerKeyOffset(topic, request.idempotencyKey());
        if (existing != null) {
            QueueMessage prior = storage.readMessage(topic, existing);
            emit(QueueEvent.Kind.PRODUCED, topic, null, existing, prior.messageId(), 0,
                    "duplicate produce idempotencyKey=" + request.idempotencyKey());
            log.info("duplicate produce topic={} offset={} messageId={} key={}",
                    topic, existing, prior.messageId(), request.idempotencyKey());
            return new EnqueueReceipt(topic, existing, prior.messageId(), EnqueueOutcome.DUPLICATE_PRODUCE);
        }

        awaitCapacity(topic, settings, waitMillis);

        String messageId = "msg-" + HexFormat.of().formatHex(randomBytes());
        QueueMessage message = new QueueMessage(topic, -1L, messageId,
                request.payload() == null ? new byte[0] : request.payload(),
                request.contentType() == null ? "application/octet-stream" : request.contentType(),
                request.headers() == null ? Map.of() : Map.copyOf(request.headers()),
                System.currentTimeMillis(), QueueMessage.CURRENT_FORMAT_VERSION);
        long offset = storage.appendMessage(topic, message);
        if (request.idempotencyKey() != null) {
            storage.putProducerKey(topic, request.idempotencyKey(), offset);
        }
        // 将业务幂等键登记到各已存在分组，供消费去重
        if (request.idempotencyKey() != null) {
            for (String group : storage.listGroups(topic)) {
                MutableGroupState s = MutableGroupState.load(storage, topic, group);
                s.idempotencyKeys.put(offset, request.idempotencyKey());
                s.save(storage);
            }
        }
        changed.signalAll();
        emit(QueueEvent.Kind.PRODUCED, topic, null, offset, messageId, 0, "accepted");
        log.info("produced topic={} offset={} messageId={}", topic, offset, messageId);
        return new EnqueueReceipt(topic, offset, messageId, EnqueueOutcome.ACCEPTED);
    }

    private void awaitCapacity(String topic, TopicSettings settings, long waitMillis)
            throws InterruptedException {
        BackpressurePolicyView policy = BackpressurePolicyView.of(settings.backpressurePolicy());
        long deadline = System.currentTimeMillis() + Math.max(0L, waitMillis);
        while (storage.depth(topic) >= settings.maxDepth()) {
            switch (policy) {
                case REJECT -> throw new QueueFullException(ErrorCode.QUEUE_FULL,
                        "topic '" + topic + "' is full (maxDepth=" + settings.maxDepth()
                                + ", depth=" + storage.depth(topic) + ")");
                case BATCH -> throw new QueueFullException(ErrorCode.QUEUE_FULL,
                        "topic '" + topic + "' is full; BATCH policy requires produceBatch() "
                                + "(maxDepth=" + settings.maxDepth() + ")");
                case DELAY -> {
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0L) {
                        throw new ProduceTimeoutException(ErrorCode.PRODUCE_TIMEOUT,
                                "timed out waiting for capacity on topic '" + topic
                                        + "' (maxDepth=" + settings.maxDepth() + ")");
                    }
                    changed.await(Math.min(remaining, Math.max(1L, properties.leaseReaperIntervalMillis())),
                            TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    // ================================================================ 消费侧

    public ReceiveResult receive(String token, String topic, String group, String consumerId,
                                 int maxMessages, long timeoutMillis) {
        lock.lock();
        try {
            checkOpenForConsume();
            require(token, Permission.CONSUME, topic);
            long deadline = System.currentTimeMillis() + Math.max(0L, timeoutMillis);
            List<Delivery> deliveries;
            while ((deliveries = collectDeliveries(topic, group, consumerId, maxMessages)).isEmpty()) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0L) {
                    // 超时稳定返回空列表（RECEIVE_TIMEOUT 语义），不抛异常以便轮询使用
                    log.info("receive timeout topic={} group={} consumer={}", topic, group, consumerId);
                    return new ReceiveResult(topic, group, List.of());
                }
                changed.await(Math.min(remaining, Math.max(1L, properties.leaseReaperIntervalMillis())),
                        TimeUnit.MILLISECONDS);
                checkOpenForConsume();
            }
            return new ReceiveResult(topic, group, deliveries);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QueueRuntimeException(ErrorCode.RECEIVE_TIMEOUT,
                    "receive interrupted for " + topic + "/" + group, e);
        } finally {
            lock.unlock();
        }
    }

    private List<Delivery> collectDeliveries(String topic, String group, String consumerId, int maxMessages) {
        MutableGroupState s = MutableGroupState.load(storage, topic, group);
        GroupSettings gs = storage.groupSettings(topic, group);
        long topicEnd = storage.nextOffset(topic);
        // 下界必须是 committedOffset：nack 重试或可见性超时回收的位点可能小于 nextOffset
        long next = s.committedOffset;
        long now = System.currentTimeMillis();
        List<Delivery> result = new ArrayList<>();

        for (long offset = next; offset < topicEnd && result.size() < Math.max(1, maxMessages); offset++) {
            final long cur = offset;
            if (s.leases.containsKey(cur)) {
                // 在途（可能是更早批量中取走的消息）：不阻塞其后续位点
                continue;
            }
            boolean terminal = cur < s.committedOffset
                    || s.ackedOffsets.contains(cur)
                    || s.deadLetters.stream().anyMatch(d -> d.offset() == cur);
            if (terminal) {
                continue;
            }
            long visibleAt = s.availableAt.getOrDefault(cur, 0L);
            if (visibleAt > now) {
                // 退避/可见性等待中：必须停止，跳过它去交付更大位点会破坏分区顺序
                break;
            }
            QueueMessage message = storage.readMessage(topic, cur);
            int attempt = s.attempts.getOrDefault(cur, 0) + 1;
            DeliveryReason reason = s.pendingReasons.getOrDefault(cur, DeliveryReason.INITIAL);
            long leasedAt = now;
            Lease lease = new Lease(cur, encodeToken(topic, group, cur), consumerId,
                    leasedAt, leasedAt + gs.visibilityTimeoutMillis(), attempt, reason);
            s.leases.put(cur, lease);
            s.attempts.put(cur, attempt);
            result.add(new Delivery(message, lease));
            emit(QueueEvent.Kind.DELIVERED, topic, group, cur, message.messageId(), attempt,
                    "reason=" + reason);
            log.info("delivered topic={} group={} offset={} messageId={} attempt={} reason={} consumer={}",
                    topic, group, cur, message.messageId(), attempt, reason, consumerId);
        }

        if (!result.isEmpty()) {
            s.nextOffset = Math.max(s.nextOffset, topicEnd);
            s.save(storage);
        }
        return result;
    }

    public CommitReceipt commit(String token, String topic, String group,
                                String deliveryToken, String idempotencyKey) {
        lock.lock();
        try {
            checkOpen();
            require(token, Permission.CONSUME, topic);
            TokenParts parts = parseToken(deliveryToken);
            if (!parts.topic().equals(topic) || !parts.group().equals(group)) {
                throw new OffsetRejectedException(ErrorCode.CROSS_GROUP_COMMIT_REJECTED,
                        "delivery token belongs to " + parts.topic() + "/" + parts.group()
                                + " but commit targeted " + topic + "/" + group
                                + "; cross-group commit is not allowed");
            }
            MutableGroupState s = MutableGroupState.load(storage, topic, group);
            long offset = parts.offset();

            // 1) 同一投递令牌重复提交优先识别为幂等成功（即便位点已被压缩推进）
            Long priorByToken = s.committedTokens.get(deliveryToken);
            if (priorByToken != null) {
                long committed = advanceCommitted(s);
                s.save(storage);
                emit(QueueEvent.Kind.DUPLICATE_COMMIT, topic, group, offset, null, 0,
                        "token already committed");
                log.info("duplicate commit (idempotent) topic={} group={} offset={}", topic, group, offset);
                return new CommitReceipt(topic, group, offset, deliveryToken,
                        CommitOutcome.ALREADY_COMMITTED, committed);
            }

            // 2) 位点回退：携带更老位点的（历史/伪造）令牌必须被明确拒绝
            if (offset < s.committedOffset) {
                throw new OffsetRejectedException(ErrorCode.OFFSET_ROLLBACK_REJECTED,
                        explain(topic, group, "offset " + offset
                                + " is already before committedOffset " + s.committedOffset
                                + "; rollback commit rejected"));
            }

            // 3) 租约有效性（含可见性超时后旧令牌作废）
            Lease lease = s.leases.get(offset);
            boolean validLease = lease != null && lease.deliveryToken().equals(deliveryToken);
            if (!validLease) {
                throw new OffsetRejectedException(ErrorCode.UNKNOWN_DELIVERY_TOKEN,
                        explain(topic, group, "unknown or superseded delivery token for offset "
                                + offset + " (lease was likely redelivered after visibility timeout)"));
            }

            String businessKey = idempotencyKey != null ? idempotencyKey
                    : s.idempotencyKeys.get(offset);
            if (businessKey != null && s.processedKeys.containsKey(businessKey)
                    && s.processedKeys.get(businessKey) != offset) {
                // 同一业务键此前已在另一位点生效：本次为重复投递，不能再次变更业务状态
                s.leases.remove(offset);
                s.ackedOffsets.add(offset);
                s.committedTokens.put(deliveryToken, offset);
                trimLru(s.committedTokens);
                long committed = persistAndCompact(s);
                emit(QueueEvent.Kind.DUPLICATE_DELIVERY, topic, group, offset, null,
                        lease.attempt(), "businessKey=" + businessKey
                                + " firstProcessedOffset=" + s.processedKeys.get(businessKey));
                log.warn("duplicate delivery suppressed topic={} group={} offset={} attempt={} businessKey={}",
                        topic, group, offset, lease.attempt(), businessKey);
                return new CommitReceipt(topic, group, offset, deliveryToken,
                        CommitOutcome.DUPLICATE_DELIVERY, committed);
            }

            // 正常提交：业务状态在本位点首次生效
            s.leases.remove(offset);
            s.availableAt.remove(offset);
            s.pendingReasons.remove(offset);
            s.ackedOffsets.add(offset);
            s.committedTokens.put(deliveryToken, offset);
            trimLru(s.committedTokens);
            if (businessKey != null) {
                s.processedKeys.put(businessKey, offset);
                trimLru(s.processedKeys);
            }
            long committedAfter = persistAndCompact(s);
            emit(QueueEvent.Kind.COMMITTED, topic, group, offset, null, lease.attempt(),
                    "committedOffset=" + committedAfter);
            log.info("committed topic={} group={} offset={} attempt={} committedOffset={}",
                    topic, group, offset, lease.attempt(), committedAfter);
            return new CommitReceipt(topic, group, offset, deliveryToken,
                    CommitOutcome.COMMITTED, committedAfter);
        } finally {
            lock.unlock();
        }
    }

    public NackReceipt nack(String token, String topic, String group, String deliveryToken,
                            String errorCode, String errorMessage, boolean retryable) {
        lock.lock();
        try {
            checkOpen();
            require(token, Permission.CONSUME, topic);
            TokenParts parts = parseToken(deliveryToken);
            if (!parts.topic().equals(topic) || !parts.group().equals(group)) {
                throw new OffsetRejectedException(ErrorCode.CROSS_GROUP_COMMIT_REJECTED,
                        "delivery token belongs to " + parts.topic() + "/" + parts.group()
                                + " but nack targeted " + topic + "/" + group);
            }
            MutableGroupState s = MutableGroupState.load(storage, topic, group);
            long offset = parts.offset();
            Lease lease = s.leases.get(offset);
            if (lease == null || !lease.deliveryToken().equals(deliveryToken)) {
                throw new OffsetRejectedException(ErrorCode.UNKNOWN_DELIVERY_TOKEN,
                        explain(topic, group, "unknown or superseded delivery token for offset "
                                + offset + " on nack"));
            }

            GroupSettings gs = storage.groupSettings(topic, group);
            RetryPolicy policy = gs.retryPolicy();
            int attemptsMade = lease.attempt();
            boolean canRetry = retryable && attemptsMade < policy.maxAttempts();

            s.leases.remove(offset);

            if (!canRetry) {
                DeadLetterReason reason = !retryable
                        ? DeadLetterReason.NON_RETRYABLE
                        : DeadLetterReason.RETRY_EXHAUSTED;
                QueueMessage message = storage.readMessage(topic, offset);
                DeadLetterRecord record = new DeadLetterRecord(topic, group, offset,
                        message.messageId(), attemptsMade, reason,
                        errorCode, errorMessage, System.currentTimeMillis());
                s.deadLetters.add(record);
                s.ackedOffsets.add(offset); // 死信亦为终态，推动位点前进
                s.availableAt.remove(offset);
                s.pendingReasons.remove(offset);
                long committedAfter = persistAndCompact(s);
                emit(QueueEvent.Kind.DEAD_LETTER, topic, group, offset, message.messageId(),
                        attemptsMade, "reason=" + reason + " error=" + errorCode
                                + " committedOffset=" + committedAfter);
                log.error("dead-letter topic={} group={} offset={} messageId={} attempts={} reason={} error={}",
                        topic, group, offset, message.messageId(), attemptsMade, reason, errorCode);
                return new NackReceipt(topic, group, offset, deliveryToken,
                        NackOutcome.DEAD_LETTER, null, null, record);
            }

            long delay = policy.delayMillisFor(attemptsMade);
            long visibleAt = System.currentTimeMillis() + delay;
            s.attempts.put(offset, attemptsMade);
            s.availableAt.put(offset, visibleAt);
            s.pendingReasons.put(offset, DeliveryReason.RETRY_AFTER_NACK);
            s.save(storage);
            changed.signalAll();
            emit(QueueEvent.Kind.NACK_RETRY, topic, group, offset, null, attemptsMade,
                    "nextVisibleAt=" + visibleAt + " delayMs=" + delay);
            log.warn("nack retry scheduled topic={} group={} offset={} attempts={} nextAttempt={} visibleAt={}",
                    topic, group, offset, attemptsMade, attemptsMade + 1, visibleAt);
            return new NackReceipt(topic, group, offset, deliveryToken,
                    NackOutcome.RETRY_SCHEDULED, attemptsMade + 1, visibleAt, null);
        } finally {
            lock.unlock();
        }
    }

    // ================================================================ 内部机制

    /** 在持锁状态下保存并压缩位点/已淘汰元数据，返回推进后的 committedOffset。 */
    private long persistAndCompact(MutableGroupState s) {
        long committed = advanceCommitted(s);
        // 压缩仅与终态位点有关的辅助映射
        s.attempts.keySet().removeIf(o -> o < s.committedOffset);
        s.availableAt.keySet().removeIf(o -> o < s.committedOffset);
        s.idempotencyKeys.keySet().removeIf(o -> o < s.committedOffset);
        s.pendingReasons.keySet().removeIf(o -> o < s.committedOffset);
        s.ackedOffsets.removeIf(o -> o < s.committedOffset);
        s.save(storage);
        changed.signalAll();
        evictIfNeeded(s.topic);
        return committed;
    }

    /** 终态（已确认 + 死信）连续时推进 committedOffset；位点不会跳变或越位。 */
    private long advanceCommitted(MutableGroupState s) {
        java.util.Set<Long> terminal = new TreeSet<>(s.ackedOffsets);
        s.deadLetters.forEach(d -> terminal.add(d.offset()));
        long expected = s.committedOffset;
        while (terminal.contains(expected)) {
            expected++;
        }
        s.committedOffset = expected;
        return expected;
    }

    /**
     * 提交后压缩主题日志：仅淘汰所有现存分组的 committedOffset 都越过、
     * 且超出主题 retentionCount 保留窗口的消息，从而在途、待重试、慢分组消息与
     * 近期可重放位点绝不丢失。深度因此始终有界（maxDepth + 有限在途量）。
     */
    private void evictIfNeeded(String topic) {
        TopicSettings settings = storage.topicSettings(topic);
        long watermark = Long.MAX_VALUE;
        for (String group : storage.listGroups(topic)) {
            watermark = Math.min(watermark, storage.loadGroupState(topic, group).committedOffset());
        }
        if (watermark == Long.MAX_VALUE) {
            // 没有任何分组：不主动淘汰（消息仍受 maxDepth 背压约束）
            return;
        }
        // 保留最近 retentionCount 条已终态消息用于回退重放
        long retain = Math.max(0L, watermark - settings.retentionCount());
        long logStart = storage.logStartOffset(topic);
        if (retain > logStart) {
            storage.evictMessagesBefore(topic, retain);
        }
    }

    private void startReaper() {
        reaperThread = new Thread(this::reaperLoop, "taskqueue-lease-reaper");
        reaperThread.setDaemon(true);
        reaperThread.start();
    }

    private void reaperLoop() {
        while (!closed.get()) {
            try {
                lock.lock();
                try {
                    boolean any = false;
                    long now = System.currentTimeMillis();
                    for (String topic : storage.listTopics()) {
                        for (String group : storage.listGroups(topic)) {
                            any |= reclaimExpiredLeases(topic, group, now, null);
                        }
                    }
                    if (any) {
                        changed.signalAll();
                    }
                    changed.await(Math.max(1L, properties.leaseReaperIntervalMillis()), TimeUnit.MILLISECONDS);
                } finally {
                    lock.unlock();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (BackendUnavailableException e) {
                log.warn("reaper skipped: backend unavailable: {}", e.getMessage());
            } catch (RuntimeException e) {
                log.error("reaper iteration failed", e);
            }
        }
    }

    /** 回收已超时在途租约（或关闭时回收全部）。返回是否发生回收。 */
    private boolean reclaimExpiredLeases(String topic, String group, long now,
                                         DeliveryReason shutdownReason) {
        MutableGroupState s = MutableGroupState.load(storage, topic, group);
        if (s.leases.isEmpty()) {
            return false;
        }
        boolean changedState = false;
        Iterator<Map.Entry<Long, Lease>> it = s.leases.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Lease> e = it.next();
            Lease lease = e.getValue();
            boolean expire = shutdownReason != null || lease.visibleAtMillis() <= now;
            if (!expire) {
                continue;
            }
            DeliveryReason reason = shutdownReason != null
                    ? shutdownReason
                    : DeliveryReason.REDISPATCH_AFTER_VISIBILITY_TIMEOUT;
            s.availableAt.put(e.getKey(), now);
            s.pendingReasons.put(e.getKey(), reason);
            it.remove();
            changedState = true;
            QueueEvent.Kind kind = shutdownReason != null
                    ? QueueEvent.Kind.SHUTDOWN_RECLAIM
                    : QueueEvent.Kind.VISIBILITY_TIMEOUT_REDISPATCH;
            emit(kind, topic, group, e.getKey(), null, lease.attempt(),
                    "leasedTo=" + lease.consumerId() + " visibleAt=" + lease.visibleAtMillis());
            log.warn("lease reclaimed topic={} group={} offset={} attempt={} reason={}",
                    topic, group, e.getKey(), lease.attempt(), reason);
        }
        if (changedState) {
            s.save(storage);
        }
        return changedState;
    }

    // ================================================================ 鉴权

    private Principal require(String token, Permission permission, String topic) {
        if (token == null || token.isBlank()) {
            throw new InvalidCredentialException(ErrorCode.INVALID_CREDENTIAL,
                    "missing credential token");
        }
        Principal principal = storage.principal(token);
        if (principal == null) {
            throw new InvalidCredentialException(ErrorCode.INVALID_CREDENTIAL,
                    "invalid credential token");
        }
        if (!principal.hasPermission(permission)) {
            throw new AuthorizationException(ErrorCode.PERMISSION_DENIED,
                    "principal lacks permission " + permission);
        }
        if (topic != null) {
            requireTopicExists(topic);
            if (!principal.topicGranted(topic)) {
                throw new AuthorizationException(ErrorCode.CROSS_TOPIC_ACCESS_DENIED,
                        "principal is not authorized for topic '" + topic + "'");
            }
        }
        return principal;
    }

    private void requireTopicExists(String topic) {
        if (!storage.listTopics().contains(topic)) {
            throw new QueueRuntimeException(ErrorCode.UNKNOWN_TOPIC,
                    "unknown topic: " + topic);
        }
    }

    // ================================================================ 令牌与工具

    private record TokenParts(String topic, String group, long offset) { }

    private String encodeToken(String topic, String group, long offset) {
        return "dt_" + URLEncoder.encode(topic, StandardCharsets.UTF_8)
                + "." + URLEncoder.encode(group, StandardCharsets.UTF_8)
                + "." + offset
                + "." + HexFormat.of().formatHex(randomBytes());
    }

    private TokenParts parseToken(String deliveryToken) {
        if (deliveryToken == null || !deliveryToken.startsWith("dt_")) {
            throw new OffsetRejectedException(ErrorCode.UNKNOWN_DELIVERY_TOKEN,
                    "malformed delivery token");
        }
        String[] parts = deliveryToken.substring(3).split("\\.");
        if (parts.length < 4) {
            throw new OffsetRejectedException(ErrorCode.UNKNOWN_DELIVERY_TOKEN,
                    "malformed delivery token");
        }
        try {
            return new TokenParts(
                    java.net.URLDecoder.decode(parts[0], StandardCharsets.UTF_8),
                    java.net.URLDecoder.decode(parts[1], StandardCharsets.UTF_8),
                    Long.parseLong(parts[2]));
        } catch (NumberFormatException e) {
            throw new OffsetRejectedException(ErrorCode.UNKNOWN_DELIVERY_TOKEN,
                    "malformed delivery token offset", e);
        }
    }

    private void trimLru(Map<String, Long> map) {
        int max = properties.idempotencyRetention();
        Iterator<String> it = map.keySet().iterator();
        while (map.size() > max && it.hasNext()) {
            it.next();
            it.remove();
        }
    }

    private static byte[] randomBytes() {
        byte[] bytes = new byte[12];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    private static byte[] joinPayloads(List<byte[]> payloads) {
        int total = payloads.stream().mapToInt(p -> p.length).sum();
        byte[] out = new byte[total];
        int pos = 0;
        for (byte[] p : payloads) {
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }

    private String explain(String topic, String group, String detail) {
        return "topic=" + topic + " group=" + group + " :: " + detail;
    }

    private void emit(QueueEvent.Kind kind, String topic, String group, long offset,
                      String messageId, int attempt, String detail) {
        QueueEvent event = new QueueEvent(kind, topic, group, offset, messageId, attempt, detail);
        for (Consumer<QueueEvent> listener : listeners) {
            try {
                listener.accept(event);
            } catch (RuntimeException e) {
                log.warn("event listener threw for {}", event, e);
            }
        }
    }

    private void checkOpen() {
        if (closed.get()) {
            throw new RuntimeClosedException(ErrorCode.RUNTIME_CLOSED,
                    "task queue runtime is closed");
        }
    }

    private void checkOpenForConsume() {
        if (closed.get()) {
            throw new RuntimeClosedException(ErrorCode.RUNTIME_CLOSED,
                    "task queue runtime is closed");
        }
        if (shuttingDown.get()) {
            throw new RuntimeClosedException(ErrorCode.SHUTDOWN_IN_PROGRESS,
                    "task queue runtime is shutting down; no new deliveries");
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    /** 主题当前保留消息深度（背压/有界性的可观测入口）。 */
    public int depth(String adminToken, String topic) {
        lock.lock();
        try {
            checkOpen();
            require(adminToken, Permission.ADMIN, topic);
            return storage.depth(topic);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        shuttingDown.set(true);
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            for (String topic : storage.listTopics()) {
                for (String group : storage.listGroups(topic)) {
                    reclaimExpiredLeases(topic, group, now, DeliveryReason.REDISPATCH_AFTER_SHUTDOWN);
                }
            }
            changed.signalAll();
            log.info("runtime closing: all in-flight messages reclaimed for redelivery");
        } finally {
            lock.unlock();
        }
        if (reaperThread != null) {
            reaperThread.interrupt();
            try {
                reaperThread.join(2_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        storage.close();
    }

    /** 仅用于内部以枚举名映射，避免直接在 switch 中引用 record 枚举造成的导入噪声。 */
    private enum BackpressurePolicyView {
        REJECT, DELAY, BATCH;

        static BackpressurePolicyView of(com.github.highcumontoa.taskqueueruntimejava.queue.model.BackpressurePolicy p) {
            return valueOf(p.name());
        }
    }
}
