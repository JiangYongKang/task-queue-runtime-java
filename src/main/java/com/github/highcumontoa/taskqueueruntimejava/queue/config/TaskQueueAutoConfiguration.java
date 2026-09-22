package com.github.highcumontoa.taskqueueruntimejava.queue.config;

import com.github.highcumontoa.taskqueueruntimejava.queue.runtime.RuntimeFactory;
import com.github.highcumontoa.taskqueueruntimejava.queue.runtime.TaskQueueRuntime;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Spring 装配：默认提供一个嵌入式运行时 Bean。 */
@Configuration
@EnableConfigurationProperties(TaskQueueSpringProperties.class)
public class TaskQueueAutoConfiguration {

    @Bean(destroyMethod = "close")
    public TaskQueueRuntime taskQueueRuntime(TaskQueueSpringProperties springProperties) {
        return RuntimeFactory.create(springProperties.toRuntimeProperties());
    }
}
