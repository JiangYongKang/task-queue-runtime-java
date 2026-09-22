package com.github.highcumontoa.taskqueueruntimejava.tests;

import com.github.highcumontoa.taskqueueruntimejava.backend.InMemoryQueueBackend;
import com.github.highcumontoa.taskqueueruntimejava.backend.QueueBackend;
import com.github.highcumontoa.taskqueueruntimejava.model.Credential;
import com.github.highcumontoa.taskqueueruntimejava.model.Permission;
import com.github.highcumontoa.taskqueueruntimejava.runtime.AccessControl;
import com.github.highcumontoa.taskqueueruntimejava.runtime.DefaultAccessControl;
import com.github.highcumontoa.taskqueueruntimejava.runtime.DefaultTaskQueueRuntime;
import com.github.highcumontoa.taskqueueruntimejava.runtime.TaskQueueRuntime;

import java.util.EnumSet;

/** 测试装配辅助：内存后端 + 可控时钟 + 预置 admin/producer/consumer 凭据。 */
final class RuntimeTestSupport {

    static final String ADMIN = "admin-token";
    static final String PRODUCER = "producer-token";
    static final String CONSUMER = "consumer-token";
    static final String OUTSIDER = "outsider-token";

    private RuntimeTestSupport() {
    }

    record Env(TaskQueueRuntime runtime, AccessControl acl, MutableClock clock,
               QueueBackend backend) {
    }

    static Env newRuntime(QueueBackend backend) {
        MutableClock clock = new MutableClock();
        DefaultAccessControl acl = new DefaultAccessControl();
        acl.register(new Credential(ADMIN, "admin", EnumSet.of(Permission.ADMIN)));
        acl.register(new Credential(PRODUCER, "producer", EnumSet.noneOf(Permission.class)));
        acl.register(new Credential(CONSUMER, "consumer", EnumSet.noneOf(Permission.class)));
        acl.register(new Credential(OUTSIDER, "outsider", EnumSet.noneOf(Permission.class)));
        TaskQueueRuntime runtime = new DefaultTaskQueueRuntime(backend, acl, clock);
        return new Env(runtime, acl, clock, backend);
    }

    static Env memoryRuntime() {
        return newRuntime(new InMemoryQueueBackend());
    }

    static void grantTopic(Env env, String topic) {
        env.runtime().grant(PRODUCER, topic, Permission.PRODUCE);
        env.runtime().grant(CONSUMER, topic, Permission.CONSUME);
    }
}
