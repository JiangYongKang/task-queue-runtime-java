package com.github.highcumontoa.taskqueueruntimejava.queue.runtime;

import com.github.highcumontoa.taskqueueruntimejava.queue.config.QueueRuntimeProperties;
import com.github.highcumontoa.taskqueueruntimejava.queue.storage.AbstractInMemoryStorage;
import com.github.highcumontoa.taskqueueruntimejava.queue.storage.LocalFileStorage;
import com.github.highcumontoa.taskqueueruntimejava.queue.storage.QueueStorage;

import java.nio.file.Path;

/** 运行时与后端工厂。 */
public final class RuntimeFactory {

    private RuntimeFactory() {
    }

    public static QueueStorage createStorage(QueueRuntimeProperties properties) {
        return switch (properties.backendType()) {
            case MEMORY -> new AbstractInMemoryStorage();
            case LOCAL_FILE -> new LocalFileStorage(Path.of(properties.dataDirectory()));
        };
    }

    public static TaskQueueRuntime create(QueueRuntimeProperties properties) {
        return new TaskQueueRuntime(createStorage(properties), properties);
    }
}
