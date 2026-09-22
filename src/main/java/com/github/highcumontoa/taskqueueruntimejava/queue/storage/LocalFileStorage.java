package com.github.highcumontoa.taskqueueruntimejava.queue.storage;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.BackendUnavailableException;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.MessageFormatException;
import com.github.highcumontoa.taskqueueruntimejava.queue.error.QueueRuntimeException;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.DeadLetterRecord;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupSettings;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.GroupState;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Lease;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.Principal;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.QueueMessage;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.RetryPolicy;
import com.github.highcumontoa.taskqueueruntimejava.queue.model.TopicSettings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 本地可恢复后端。
 *
 * 目录布局：
 * <pre>
 *   dataDir/
 *     principals.json
 *     topics/
 *       &lt;topic&gt;/
 *         meta.json              主题设置 + 位点边界
 *         messages.log           JSONL，每行一条 v2 消息（append-only，fsync）
 *         groups/
 *           &lt;group&gt;.json         分组消费状态的原子快照
 * </pre>
 * 旧版本（v1，无 contentType/headers/formatVersion 字段）消息在启动时被识别并迁移为
 * 等价的 v2 表示；高于当前版本的格式抛 {@link ErrorCode#UNKNOWN_MESSAGE_FORMAT} 明确拒绝。
 */
public class LocalFileStorage extends AbstractInMemoryStorage {

    static final int WIRE_VERSION = 2;
    private static final int LEGACY_V1 = 1;

    private final Path root;
    private final ObjectMapper mapper;
    private volatile boolean available = true;

    public LocalFileStorage(Path dataDirectory) {
        this.root = dataDirectory;
        this.mapper = new ObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            Files.createDirectories(root.resolve("topics"));
            recover();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to initialize local storage at " + root, e);
        }
    }

    /** 故障注入：置为 false 后所有读写抛 {@link ErrorCode#BACKEND_UNAVAILABLE}。 */
    public void setAvailable(boolean available) {
        this.available = available;
    }

    private void ensureAvailable() {
        if (!available) {
            throw new BackendUnavailableException(ErrorCode.BACKEND_UNAVAILABLE,
                    "local storage backend is unavailable (fault injection)");
        }
    }

    @Override
    public boolean isRecoverable() {
        return true;
    }

    // ---------------------------------------------------------------- 恢复

    private void recover() throws IOException {
        Path principalsFile = root.resolve("principals.json");
        if (Files.exists(principalsFile)) {
            PrincipalWire wire = mapper.readValue(Files.readString(principalsFile), PrincipalWire.class);
            if (wire.principals != null) {
                wire.principals.forEach(p -> super.registerPrincipal(p));
            }
        }
        Path topicsDir = root.resolve("topics");
        if (!Files.isDirectory(topicsDir)) {
            return;
        }
        try (Stream<Path> topicDirs = Files.list(topicsDir)) {
            topicDirs.filter(Files::isDirectory).forEach(this::recoverTopic);
        }
    }

    private void recoverTopic(Path topicDir) {
        try {
            String topic = topicDir.getFileName().toString();
            Path metaFile = topicDir.resolve("meta.json");
            if (!Files.exists(metaFile)) {
                return;
            }
            MetaWire meta = mapper.readValue(Files.readString(metaFile), MetaWire.class);
            TopicSettings settings = new TopicSettings(meta.maxDepth,
                    com.github.highcumontoa.taskqueueruntimejava.queue.model.BackpressurePolicy
                            .valueOf(meta.backpressurePolicy));

            TopicStore store = new TopicStore();
            store.settings = settings;
            store.nextOffset = meta.nextOffset;
            store.logStartOffset = meta.logStartOffset;
            if (meta.producerKeys != null) {
                store.producerKeys.putAll(meta.producerKeys);
            }

            Path messagesFile = topicDir.resolve("messages.log");
            if (Files.exists(messagesFile)) {
                try (BufferedReader reader = Files.newBufferedReader(messagesFile)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.isBlank()) {
                            continue;
                        }
                        QueueMessage migrated = migrate(mapper.readValue(line, MessageWire.class), topic);
                        store.messages.put(migrated.offset(), migrated);
                    }
                }
            }
            topics.put(topic, store);

            Path groupsDir = topicDir.resolve("groups");
            if (Files.isDirectory(groupsDir)) {
                try (Stream<Path> groupFiles = Files.list(groupsDir)) {
                    groupFiles.filter(p -> p.toString().endsWith(".json")).forEach(gf -> recoverGroup(store, gf));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to recover topic directory " + topicDir, e);
        }
    }

    private void recoverGroup(TopicStore store, Path groupFile) {
        try {
            String group = groupFile.getFileName().toString().replace(".json", "");
            GroupWire wire = mapper.readValue(Files.readString(groupFile), GroupWire.class);
            GroupStore gs = new GroupStore();
            RetryPolicy rp = wire.retry == null
                    ? GroupSettings.defaults().retryPolicy()
                    : new RetryPolicy(wire.retry.maxAttempts, wire.retry.baseDelayMillis,
                            wire.retry.multiplier, wire.retry.maxDelayMillis);
            gs.settings = new GroupSettings(wire.visibilityTimeoutMillis, rp);
            gs.committedOffset = wire.committedOffset;
            gs.nextOffset = wire.nextOffset;
            if (wire.attempts != null) {
                wire.attempts.forEach((k, v) -> gs.attempts.put(Long.valueOf(k), v));
            }
            if (wire.availableAt != null) {
                wire.availableAt.forEach((k, v) -> gs.availableAt.put(Long.valueOf(k), v));
            }
            if (wire.idempotencyKeys != null) {
                wire.idempotencyKeys.forEach((k, v) -> gs.idempotencyKeys.put(Long.valueOf(k), v));
            }
            if (wire.pendingReasons != null) {
                wire.pendingReasons.forEach((k, v) -> gs.pendingReasons.put(Long.valueOf(k),
                        com.github.highcumontoa.taskqueueruntimejava.queue.model.DeliveryReason.valueOf(v)));
            }
            if (wire.ackedOffsets != null) {
                wire.ackedOffsets.forEach(o -> gs.ackedOffsets.add(o));
            }
            if (wire.committedTokens != null) {
                wire.committedTokens.forEach(gs.committedTokens::put);
            }
            if (wire.processedKeys != null) {
                wire.processedKeys.forEach(gs.processedKeys::put);
            }
            if (wire.leases != null) {
                wire.leases.forEach(l -> gs.leases.put(l.offset,
                        new Lease(l.offset, l.deliveryToken, l.consumerId, l.leasedAtMillis,
                                l.visibleAtMillis, l.attempt,
                                com.github.highcumontoa.taskqueueruntimejava.queue.model.DeliveryReason
                                        .valueOf(l.reason))));
            }
            if (wire.deadLetters != null) {
                wire.deadLetters.forEach(d -> gs.deadLetters.add(new DeadLetterRecord(d.topic, d.group,
                        d.offset, d.messageId, d.attempts,
                        com.github.highcumontoa.taskqueueruntimejava.queue.model.DeadLetterReason
                                .valueOf(d.reason),
                        d.errorCode, d.errorMessage, d.deadAtMillis)));
            }
            store.groups.put(group, gs);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to recover group file " + groupFile, e);
        }
    }

    /** v1 → v2：缺失字段补默认值；未知高版本明确拒绝。 */
    private QueueMessage migrate(MessageWire wire, String fallbackTopic) {
        int version = wire.formatVersion == null ? LEGACY_V1 : wire.formatVersion;
        if (version > QueueMessage.CURRENT_FORMAT_VERSION) {
            throw new MessageFormatException(ErrorCode.UNKNOWN_MESSAGE_FORMAT,
                    "message at offset " + wire.offset + " uses unsupported format version " + version
                            + " (supported up to " + QueueMessage.CURRENT_FORMAT_VERSION + ")");
        }
        Map<String, String> headers = wire.headers == null ? Map.of() : Map.copyOf(wire.headers);
        String contentType = wire.contentType != null ? wire.contentType : "application/octet-stream";
        byte[] payload = wire.payload != null ? wire.payload : new byte[0];
        return new QueueMessage(wire.topic != null ? wire.topic : fallbackTopic,
                wire.offset, wire.messageId, payload, contentType, headers,
                wire.enqueueTimeMillis, QueueMessage.CURRENT_FORMAT_VERSION);
    }

    // ---------------------------------------------------------------- 持久化钩子

    @Override
    protected void afterMessagesEvicted(String topic, long newLogStartOffset) {
        compactMessagesLog(topic);
    }

    @Override
    public synchronized void putProducerKey(String topic, String idempotencyKey, long offset) {
        super.putProducerKey(topic, idempotencyKey, offset);
        writeMeta(topic);
    }

    /** 将内存中仍保留的消息重写为新的 messages.log（原子替换），完成日志压缩。 */
    private synchronized void compactMessagesLog(String topic) {
        ensureAvailable();
        try {
            TopicStore store = requireTopic(topic);
            Path messagesFile = topicDir(topic).resolve("messages.log");
            Path tmp = messagesFile.resolveSibling("messages.log.tmp");
            StringBuilder sb = new StringBuilder();
            for (QueueMessage message : store.messages.values()) {
                sb.append(mapper.writeValueAsString(toWire(message))).append(System.lineSeparator());
            }
            Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            try {
                Files.move(tmp, messagesFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicFailed) {
                Files.move(tmp, messagesFile, StandardCopyOption.REPLACE_EXISTING);
            }
            writeMeta(topic);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to compact message log for " + topic, e);
        }
    }

    @Override
    public void registerPrincipal(Principal principal) {
        ensureAvailable();
        super.registerPrincipal(principal);
        persistPrincipals();
    }

    @Override
    protected void afterTopicCreated(String topic, TopicSettings settings) {
        ensureAvailable();
        try {
            Path topicDir = topicDir(topic);
            Files.createDirectories(topicDir.resolve("groups"));
            writeMeta(topic);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void afterGroupCreated(String topic, String group, GroupSettings settings) {
        ensureAvailable();
        writeGroupFile(loadGroupState(topic, group), settings);
    }

    @Override
    protected void afterMessageAppended(QueueMessage message) {
        ensureAvailable();
        try {
            Path messagesFile = topicDir(message.topic()).resolve("messages.log");
            String line = mapper.writeValueAsString(toWire(message)) + System.lineSeparator();
            synchronized (this) {
                Files.writeString(messagesFile, line, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                writeMeta(message.topic());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void afterGroupStateSaved(GroupState state) {
        ensureAvailable();
        writeGroupFile(state, groupSettings(state.topic(), state.group()));
    }

    private synchronized void writeMeta(String topic) {
        TopicStore store = requireTopic(topic);
        MetaWire meta = new MetaWire();
        meta.version = WIRE_VERSION;
        meta.maxDepth = store.settings.maxDepth();
        meta.backpressurePolicy = store.settings.backpressurePolicy().name();
        meta.nextOffset = store.nextOffset;
        meta.logStartOffset = store.logStartOffset;
        meta.producerKeys = new LinkedHashMap<>(store.producerKeys);
        atomicWriteJson(topicDir(topic).resolve("meta.json"), meta);
    }

    private void writeGroupFile(GroupState state, GroupSettings settings) {
        GroupWire wire = new GroupWire();
        wire.version = WIRE_VERSION;
        wire.visibilityTimeoutMillis = settings.visibilityTimeoutMillis();
        wire.retry = new RetryWire();
        wire.retry.maxAttempts = settings.retryPolicy().maxAttempts();
        wire.retry.baseDelayMillis = settings.retryPolicy().baseDelayMillis();
        wire.retry.multiplier = settings.retryPolicy().multiplier();
        wire.retry.maxDelayMillis = settings.retryPolicy().maxDelayMillis();
        wire.committedOffset = state.committedOffset();
        wire.nextOffset = state.nextOffset();
        wire.attempts = new LinkedHashMap<>();
        state.perOffsetAttempts().forEach((k, v) -> wire.attempts.put(k.toString(), v));
        wire.availableAt = new LinkedHashMap<>();
        state.perOffsetAvailableAt().forEach((k, v) -> wire.availableAt.put(k.toString(), v));
        wire.idempotencyKeys = new LinkedHashMap<>();
        state.perOffsetIdempotencyKey().forEach((k, v) -> wire.idempotencyKeys.put(k.toString(), v));
        wire.pendingReasons = new LinkedHashMap<>();
        state.pendingReasons().forEach((k, v) -> wire.pendingReasons.put(k.toString(), v.name()));
        wire.ackedOffsets = new ArrayList<>(state.ackedOffsets());
        wire.committedTokens = new LinkedHashMap<>(state.committedTokens());
        wire.processedKeys = new LinkedHashMap<>(state.processedKeys());
        wire.leases = new ArrayList<>();
        state.leases().values().forEach(l -> {
            LeaseWire lw = new LeaseWire();
            lw.offset = l.offset();
            lw.deliveryToken = l.deliveryToken();
            lw.consumerId = l.consumerId();
            lw.leasedAtMillis = l.leasedAtMillis();
            lw.visibleAtMillis = l.visibleAtMillis();
            lw.attempt = l.attempt();
            lw.reason = l.reason().name();
            wire.leases.add(lw);
        });
        wire.deadLetters = new ArrayList<>();
        state.deadLetters().forEach(d -> {
            DeadLetterWire dw = new DeadLetterWire();
            dw.topic = d.topic();
            dw.group = d.group();
            dw.offset = d.offset();
            dw.messageId = d.messageId();
            dw.attempts = d.attempts();
            dw.reason = d.reason().name();
            dw.errorCode = d.errorCode();
            dw.errorMessage = d.errorMessage();
            dw.deadAtMillis = d.deadAtMillis();
            wire.deadLetters.add(dw);
        });
        atomicWriteJson(topicDir(state.topic()).resolve("groups").resolve(state.group() + ".json"), wire);
    }

    private synchronized void persistPrincipals() {
        PrincipalWire wire = new PrincipalWire();
        wire.principals = new ArrayList<>(principals.values());
        atomicWriteJson(root.resolve("principals.json"), wire);
    }

    private void atomicWriteJson(Path target, Object value) {
        ensureAvailable();
        try {
            Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value);
            Files.writeString(tmp, json, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            try {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicFailed) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist " + target, e);
        }
    }

    private Path topicDir(String topic) {
        return root.resolve("topics").resolve(topic);
    }

    private MessageWire toWire(QueueMessage m) {
        MessageWire wire = new MessageWire();
        wire.formatVersion = m.formatVersion();
        wire.topic = m.topic();
        wire.offset = m.offset();
        wire.messageId = m.messageId();
        wire.payload = m.payload();
        wire.contentType = m.contentType();
        wire.headers = m.headers();
        wire.enqueueTimeMillis = m.enqueueTimeMillis();
        return wire;
    }

    // ---------------------------------------------------------------- 传输 DTO

    public static final class MessageWire {
        public Integer formatVersion;
        public String topic;
        public long offset;
        public String messageId;
        public byte[] payload;
        public String contentType;
        public Map<String, String> headers;
        public long enqueueTimeMillis;
    }

    public static final class MetaWire {
        public int version;
        public int maxDepth;
        public String backpressurePolicy;
        public long nextOffset;
        public long logStartOffset;
        public Map<String, Long> producerKeys;
    }

    public static final class RetryWire {
        public int maxAttempts;
        public long baseDelayMillis;
        public double multiplier;
        public long maxDelayMillis;
    }

    public static final class LeaseWire {
        public long offset;
        public String deliveryToken;
        public String consumerId;
        public long leasedAtMillis;
        public long visibleAtMillis;
        public int attempt;
        public String reason;
    }

    public static final class DeadLetterWire {
        public String topic;
        public String group;
        public long offset;
        public String messageId;
        public int attempts;
        public String reason;
        public String errorCode;
        public String errorMessage;
        public long deadAtMillis;
    }

    public static final class GroupWire {
        public int version;
        public long visibilityTimeoutMillis;
        public RetryWire retry;
        public long committedOffset;
        public long nextOffset;
        public Map<String, Integer> attempts;
        public Map<String, Long> availableAt;
        public Map<String, String> idempotencyKeys;
        public Map<String, String> pendingReasons;
        public List<Long> ackedOffsets;
        public Map<String, Long> committedTokens;
        public Map<String, Long> processedKeys;
        public List<LeaseWire> leases;
        public List<DeadLetterWire> deadLetters;
    }

    public static final class PrincipalWire {
        public List<Principal> principals;
    }
}
