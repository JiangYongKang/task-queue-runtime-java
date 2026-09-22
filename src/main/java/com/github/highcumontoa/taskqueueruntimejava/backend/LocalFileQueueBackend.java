package com.github.highcumontoa.taskqueueruntimejava.backend;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.error.QueueException;
import com.github.highcumontoa.taskqueueruntimejava.model.TopicState;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * 本地可恢复后端：内存状态 + 每主题单一 JSON 文件的原子快照落盘。
 *
 * 持久化协议：先写 {@code <topic>.json.tmp}，强制刷盘，再原子 rename
 * 覆盖正式文件。持久化在主题锁内发生，因此“内存变更 + 落盘”对外原子；
 * 落盘失败（后端不可用）抛 BACKEND_UNAVAILABLE 且不修改内存状态语义
 * （调用方在 action 内先 persist 再完成状态变更，失败整体抛出）。
 *
 * 启动时扫描目录：仅加载 formatVersion == SUPPORTED_FORMAT_VERSION 的文件，
 * 其它版本以 UNSUPPORTED_FORMAT 明确拒绝，禁止字段缺失下的不确定行为。
 */
public class LocalFileQueueBackend extends InMemoryQueueBackend {

    static final String FILE_SUFFIX = ".topic.json";
    private static final String TMP_SUFFIX = ".tmp";

    private final Path directory;
    private final ObjectMapper mapper;

    public LocalFileQueueBackend(Path directory) {
        this.directory = directory;
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        this.mapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS);
        this.mapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
    }

    /** 从目录恢复全部主题；目录不存在则创建。 */
    public void recover() {
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new QueueException(ErrorCode.BACKEND_UNAVAILABLE,
                    "无法创建后端目录: " + directory, e);
        }
        try (Stream<Path> files = Files.list(directory)) {
            files.filter(p -> p.getFileName().toString().endsWith(FILE_SUFFIX))
                 .forEach(this::loadFile);
        } catch (IOException e) {
            throw new QueueException(ErrorCode.BACKEND_UNAVAILABLE, "扫描后端目录失败", e);
        }
    }

    private void loadFile(Path file) {
        TopicState state;
        try {
            state = mapper.readValue(Files.readAllBytes(file), TopicState.class);
        } catch (IOException e) {
            throw new QueueException(ErrorCode.UNSUPPORTED_FORMAT,
                    "主题数据无法解析: " + file.getFileName(), e);
        }
        if (state == null || state.getName() == null || state.getConfig() == null) {
            throw new QueueException(ErrorCode.UNSUPPORTED_FORMAT,
                    "主题数据缺少必需字段: " + file.getFileName());
        }
        if (state.getFormatVersion() != TopicState.SUPPORTED_FORMAT_VERSION) {
            throw new QueueException(ErrorCode.UNSUPPORTED_FORMAT,
                    "不支持的消息格式版本 " + state.getFormatVersion()
                            + "（支持版本 " + TopicState.SUPPORTED_FORMAT_VERSION + "）: "
                            + file.getFileName());
        }
        createTopic(state.getName(), state);
    }

    @Override
    public void createTopic(String topic, TopicState state) {
        super.createTopic(topic, state);
        if (Files.exists(pathFor(topic))) {
            // 恢复路径：文件已存在，无需立即重写。
            return;
        }
        persist(state);
    }

    @Override
    public <T> T mutate(String topic, Function<TopicState, T> action) {
        // 用数组捕获 action 结果，保证先落盘再“返回”，异常时不产生对外可见的半更新。
        Object[] result = new Object[1];
        return super.mutate(topic, state -> {
            T t = action.apply(state);
            result[0] = t;
            persist(state);
            return t;
        });
    }

    /** 原子持久化：tmp + fsync + rename。调用方必须已持有主题锁。 */
    void persist(TopicState state) {
        checkOpen();
        Path target = pathFor(state.getName());
        Path tmp = pathFor(state.getName() + TMP_SUFFIX);
        try {
            byte[] bytes = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(state);
            try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(
                    tmp, java.nio.file.StandardOpenOption.WRITE,
                    java.nio.file.StandardOpenOption.CREATE,
                    java.nio.file.StandardOpenOption.TRUNCATE_EXISTING)) {
                java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(bytes);
                while (buf.hasRemaining()) {
                    ch.write(buf);
                }
                ch.force(true);
            }
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new QueueException(ErrorCode.BACKEND_UNAVAILABLE,
                    "后端持久化失败，主题: " + state.getName(), e);
        } catch (UncheckedIOException e) {
            throw new QueueException(ErrorCode.BACKEND_UNAVAILABLE,
                    "后端持久化失败，主题: " + state.getName(), e.getCause());
        }
    }

    private Path pathFor(String name) {
        return directory.resolve(name.replace("/", "_") + FILE_SUFFIX);
    }
}
