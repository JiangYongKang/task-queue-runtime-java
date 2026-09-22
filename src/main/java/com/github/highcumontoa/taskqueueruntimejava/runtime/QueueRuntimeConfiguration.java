package com.github.highcumontoa.taskqueueruntimejava.runtime;

import com.github.highcumontoa.taskqueueruntimejava.backend.InMemoryQueueBackend;
import com.github.highcumontoa.taskqueueruntimejava.backend.LocalFileQueueBackend;
import com.github.highcumontoa.taskqueueruntimejava.backend.QueueBackend;
import com.github.highcumontoa.taskqueueruntimejava.model.Credential;
import com.github.highcumontoa.taskqueueruntimejava.model.Permission;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.time.Clock;
import java.util.EnumSet;

/**
 * 运行时装配。
 * <ul>
 *   <li>{@code queue.backend=memory}（默认）：纯内存后端；</li>
 *   <li>{@code queue.backend=file} 且 {@code queue.data-dir} 非空：
 *       本地可恢复后端，启动时从目录恢复，重启后位点与消息状态不丢不跳。</li>
 * </ul>
 * 两种后端实现同一套语义接口。
 */
@Configuration
public class QueueRuntimeConfiguration {

    @Value("${queue.backend:memory}")
    private String backendType;

    @Value("${queue.data-dir:}")
    private String dataDir;

    @Bean
    public Clock queueClock() {
        return Clock.systemUTC();
    }

    @Bean(destroyMethod = "")
    public QueueBackend queueBackend() {
        if ("file".equalsIgnoreCase(backendType) && dataDir != null && !dataDir.isBlank()) {
            LocalFileQueueBackend backend = new LocalFileQueueBackend(Path.of(dataDir));
            backend.recover();
            return backend;
        }
        return new InMemoryQueueBackend();
    }

    @Bean
    public AccessControl accessControl() {
        DefaultAccessControl acl = new DefaultAccessControl();
        // 内置管理员引导凭据，仅用于本地演示与初始化；生产使用应替换为自定义注册。
        acl.register(new Credential("admin-token", "bootstrap-admin",
                EnumSet.of(Permission.ADMIN)));
        return acl;
    }

    @Bean(destroyMethod = "")
    public TaskQueueRuntime taskQueueRuntime(QueueBackend backend, AccessControl acl, Clock clock) {
        return new DefaultTaskQueueRuntime(backend, acl, clock);
    }
}
