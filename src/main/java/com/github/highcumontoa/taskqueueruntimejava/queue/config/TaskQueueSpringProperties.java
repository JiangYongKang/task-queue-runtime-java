package com.github.highcumontoa.taskqueueruntimejava.queue.config;

import com.github.highcumontoa.taskqueueruntimejava.queue.model.BackpressurePolicy;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** application.properties 前缀：taskqueue。 */
@ConfigurationProperties(prefix = "taskqueue")
public class TaskQueueSpringProperties {

    private String backend = "memory";
    private String dataDirectory = "build/taskqueue-data";
    private int defaultTopicMaxDepth = 10_000;
    private BackpressurePolicy defaultBackpressurePolicy = BackpressurePolicy.REJECT;
    private long defaultVisibilityTimeoutMillis = 30_000L;
    private long defaultProduceWaitMillis = 1_000L;
    private long leaseReaperIntervalMillis = 100L;
    private int idempotencyRetention = 10_000;

    public String getBackend() { return backend; }
    public void setBackend(String backend) { this.backend = backend; }
    public String getDataDirectory() { return dataDirectory; }
    public void setDataDirectory(String dataDirectory) { this.dataDirectory = dataDirectory; }
    public int getDefaultTopicMaxDepth() { return defaultTopicMaxDepth; }
    public void setDefaultTopicMaxDepth(int v) { this.defaultTopicMaxDepth = v; }
    public BackpressurePolicy getDefaultBackpressurePolicy() { return defaultBackpressurePolicy; }
    public void setDefaultBackpressurePolicy(BackpressurePolicy v) { this.defaultBackpressurePolicy = v; }
    public long getDefaultVisibilityTimeoutMillis() { return defaultVisibilityTimeoutMillis; }
    public void setDefaultVisibilityTimeoutMillis(long v) { this.defaultVisibilityTimeoutMillis = v; }
    public long getDefaultProduceWaitMillis() { return defaultProduceWaitMillis; }
    public void setDefaultProduceWaitMillis(long v) { this.defaultProduceWaitMillis = v; }
    public long getLeaseReaperIntervalMillis() { return leaseReaperIntervalMillis; }
    public void setLeaseReaperIntervalMillis(long v) { this.leaseReaperIntervalMillis = v; }
    public int getIdempotencyRetention() { return idempotencyRetention; }
    public void setIdempotencyRetention(int v) { this.idempotencyRetention = v; }

    public QueueRuntimeProperties toRuntimeProperties() {
        return new QueueRuntimeProperties(
                "local-file".equalsIgnoreCase(backend)
                        ? QueueRuntimeProperties.BackendType.LOCAL_FILE
                        : QueueRuntimeProperties.BackendType.MEMORY,
                dataDirectory,
                defaultTopicMaxDepth,
                defaultBackpressurePolicy,
                defaultVisibilityTimeoutMillis,
                defaultProduceWaitMillis,
                leaseReaperIntervalMillis,
                idempotencyRetention);
    }
}
