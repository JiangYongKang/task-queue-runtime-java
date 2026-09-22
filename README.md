# task-queue-runtime-java

可嵌入的**本地**任务队列运行时（Spring Boot 3 / Java 21），无外部消息服务、无真实凭据、
无付费资源，所有验证均可在本地通过 `mvn test` 完成。

## 能力

- 主题 / 消费者组：同组单消费者排他、可见性超时后自动重新投递、优雅关闭交还在途消息
- 至少一次投递 + 业务幂等去重：重复投递 / 重复提交 / 合法重试结论可区分
- 指数退避重试（有上限）+ 可查询死信（`RETRY_EXHAUSTED` / `NON_RETRYABLE`）
- 位点持久化与回退重放；位点回退、跨组提交、越界等非法操作原因可区分
- 生产 / 消费双侧令牌权限：无效凭据、权限不足、越权跨主题三码分立
- 有界队列与 REJECT / DELAY / BATCH 背压，稳定的超时 / 容量 / 后端不可用错误
- 内存后端与本地可恢复后端语义一致；旧 v1 消息格式可迁移，未知高版本明确拒绝
- 并发安全（单锁串行状态变更），无半更新 / 双生效 / 位点错乱

## 快速开始

```bash
mvn test          # 本地运行全部 22 个测试，无外部依赖
mvn package       # 打包
```

## 配置（application.properties，均为默认值）

```properties
taskqueue.backend=memory
taskqueue.data-directory=build/taskqueue-data
taskqueue.default-topic-max-depth=10000
taskqueue.default-backpressure-policy=REJECT
taskqueue.default-visibility-timeout-millis=30000
taskqueue.default-produce-wait-millis=1000
taskqueue.lease-reaper-interval-millis=100
taskqueue.idempotency-retention=10000
```

`taskqueue.backend=local-file` 时状态写入 `taskqueue.data-directory`，重启可恢复。

## REST

所有接口需 `X-Queue-Token` 头。管理员默认令牌 `local-admin-token`（仅限本地/测试）。
接口前缀 `/api/queue`：主题、分组、凭据、消息生产/批量生产、接收、提交、nack、
位点查询/重置、死信查询。

详细投递语义、重试与死信规则、权限模型、背压策略、错误码与本地验证方法见：
[`docs/DELIVERY-SEMANTICS.md`](docs/DELIVERY-SEMANTICS.md)
