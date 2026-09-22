# 本地可嵌入任务队列运行时（task-queue-runtime-java）

一个**不依赖任何外部消息服务、真实凭据或付费资源**、可直接嵌入 Spring Boot
应用的本地任务队列运行时。提供至少一次投递、消费幂等、可见性超时重投、
退避重试与死信、位点持久化与显式重放、主题级权限、有界背压，以及
**内存后端与本地可恢复文件后端的一致语义**。

所有验证均可在本地完成：`mvn test`（24 个用例，无需任何外部依赖）。

---

## 1. 投递语义

- **至少一次投递（at-least-once）**：消息在提交（commit）前可能被重复投递；
  消费端必须配合幂等机制（运行时已内置 `IdempotencyGuard` 与 `process` 模板）。
- **同组单消费者互斥**：同一消费者组内，一条消息在任一时刻至多有一个生效投递。
  复合操作（挑选消息→置在途→返回）在**主题级互斥锁**内原子完成，
  并发消费者不会重复拿到同一条消息。
- **可见性超时（visibility timeout）**：消息被拉取后进入在途状态并获得租约。
  若在租约到期前未提交/未 nack，`reclaimExpired` 会将其安全收回为可投递，
  **不会永久丢失**，下次投递原因为 `VISIBILITY_TIMEOUT`。
- **投递原因可区分**（`Delivery.reason`）：
  - `FIRST`：首次投递；
  - `RETRY`：消费者显式失败后按退避到期的**合法重试**；
  - `VISIBILITY_TIMEOUT`：未提交导致租约过期后的**重复投递**；
  - `REPLAY`：管理员显式位点重放。
- **提交结论可区分**（`CommitResult.outcome`）：
  - `COMMITTED`：首次提交成功；
  - `ALREADY_COMMITTED`：同一投递重复提交，幂等成功但结论可区分；
  - 针对已失效旧投递的提交 → 错误 `DELIVERY_STALE`（409）；
    属于其它组的投递 → `CROSS_GROUP_COMMIT_REJECTED`（409）。
- **消费侧幂等**：`process(token, topic, group, handler)` 模板保证业务副作用
  只生效一次。重复投递命中已生效记录时返回 `ALREADY_PROCESSED`，
  **业务不再执行**；生产侧可用 `producerKey` 去重，回执 `duplicate=true`。

## 2. 失败重试与死信

- 消费者通过 `nack(deliveryId, error, retryable)` 显式上报失败。
- **退避策略可配置**：第 n 次失败后的等待
  `delay = min(initial * multiplier^(n-1), max)`。
- **失败次数有上限** `maxAttempts`；达到上限 → 死信，原因 `RETRIES_EXHAUSTED`。
- 消费者声明 `retryable=false` → 立即死信，原因 `NON_RETRYABLE`。
  两种死信原因在 `DeadLetterRecord.cause` 中**可区分**。
- 死信进入可查询列表（`deadLetters(topic)` /
  `GET /api/queue/topics/{topic}/dead-letters`），**不静默丢弃、不无限重试**，
  且不再被投递。

## 3. 位点与重放管理

- 位点语义：初始已提交位点为 **-1**（下一条 offset=0）。commit 只允许
  **单调推进**，位点与消息状态在文件后端持久化，**重启后不跳变、不越位**。
- **显式重放**（管理员）：`replay(group, targetOffset)` 将
  `(targetOffset, committed]` 区间的已提交消息重置为可投递并回退位点；
  允许 `-1` 表示从头重放，重放消息的投递原因为 `REPLAY`。
- **非法操作被明确拒绝且原因可区分**：
  - 目标位点超过当前位点 → `BAD_REQUEST`（拒绝越位前进）；
  - 目标位点等于当前位点（无变化回退）→ `OFFSET_ROLLBACK_REJECTED`（409）；
  - 用其它组的投递提交 → `CROSS_GROUP_COMMIT_REJECTED`（409）。

## 4. 权限模型

- 凭据通过 token 标识（HTTP 头 `X-Queue-Token`）。
- 三类拒绝，错误码与 HTTP 状态均**可区分**：
  - 无/未知 token → `INVALID_CREDENTIAL`（401）；
  - 凭据有效但对该主题无任何授权 → `CROSS_TOPIC_ACCESS`（403）；
  - 对该主题有授权但缺少所需权限 → `PERMISSION_DENIED`（403）。
- 权限：`PRODUCE`、`CONSUME`、`ADMIN`，按主题授予；`ADMIN` 为全局权限。
- 授权相互独立：拒绝某主题的越权访问**不影响其它合法主题**。
- 安全边界：ACL 保存在进程内，不随消息文件落盘；重启后需重新授权。
- 应用默认引导一个仅用于本地演示的管理员凭据 `admin-token`，
  嵌入真实系统时应自行注册凭据替换它。

## 5. 有界队列与背压

- 每个主题配置硬性容量上限，按**未终结消息数**计数；任何情况下不允许无界占用内存。
- 积压时按主题配置的策略处理：
  - `REJECT`（默认）：立即失败，错误码 `QUEUE_FULL`（HTTP 429）；
  - `WAIT`：在 `backpressureTimeout` 窗口内等待容量，期间释放主题锁，
    消费者提交释放容量后生产者被唤醒；超时 → `BACKPRESSURE_TIMEOUT`（429）。
- 其它稳定可区分错误：`OPERATION_TIMEOUT`（408）、
  `BACKEND_UNAVAILABLE`（503，落盘失败）、`RUNTIME_CLOSED`（503）。

## 6. 后端替换与格式兼容

| 后端 | 类 | 持久化 | 适用 |
| --- | --- | --- | --- |
| 内存 | `InMemoryQueueBackend` | 否 | 测试、临时运行 |
| 本地文件 | `LocalFileQueueBackend` | 每主题单 JSON 快照 | 本地可恢复运行 |

- 两者实现同一 `QueueBackend` 接口与同一套语义（已有用例断言结论一致）。
- 文件后端写入协议：临时文件 → `force(true)` 刷盘 → 原子 `rename`，
  在主题锁内完成，避免半更新文件；启动时扫描目录恢复。
- **格式版本**：仅加载 `formatVersion == SUPPORTED_FORMAT_VERSION(1)` 的数据；
  旧版本/损坏文件以 `UNSUPPORTED_FORMAT` **明确拒绝**，
  不会因字段缺失产生不确定行为。

## 7. 并发与生命周期

- 生产、消费、提交、nack、重投、死信迁移全部在每主题互斥锁内原子完成，
  杜绝半更新、双重生效与位点错乱。
- `close(gracefulWait)`：先在宽限期内等待在途消息完成；仍未完成的在途消息
  **安全交还为可投递**（下次以超时重投处理），随后拒绝新操作。
  在途数以持久化状态为准，重启恢复后同样正确。

## 8. 本地验证方法

```bash
# 运行全部测试（无需外部服务）
mvn test

# 仅运行重点场景
mvn test -Dtest=DeliveryAndVisibilityTest   # 重复投递 / 可见性超时 / 幂等
mvn test -Dtest=RetryAndDeadLetterTest      # 退避重试 / 重试耗尽 / 死信
mvn test -Dtest=OffsetAndReplayTest         # 位点回退 / 跨组提交 / 重启恢复
mvn test -Dtest=AccessControlTest           # 越权 / 无效凭据
mvn test -Dtest=BackpressureTest            # 背压 REJECT/WAIT
mvn test -Dtest=ConcurrencyLifecycleTest    # 并发互斥 / 关闭交还
mvn test -Dtest=BackendCompatibilityTest    # 双后端语义一致 / 旧格式拒绝
mvn test -Dtest=RestApiTest                 # HTTP 闭环与稳定错误码
```

测试使用可控时钟（`MutableClock`）确定性地推进时间以触发超时/退避；
日志统一输出 `messageId / group / offset / attempt / reason`，便于追踪。

### 嵌入式使用

```java
AccessControl acl = new DefaultAccessControl();
acl.register(new Credential("admin", "admin", EnumSet.of(Permission.ADMIN)));
TaskQueueRuntime q = new DefaultTaskQueueRuntime(
        new InMemoryQueueBackend(), acl, Clock.systemUTC());

q.createTopic("admin", "orders", TopicConfig.defaults());
q.createGroup("admin", "orders", "workers");
q.grant("producer", "orders", Permission.PRODUCE);
q.grant("consumer", "orders", Permission.CONSUME);

var receipt = q.produce("producer", "orders", "payload", "dedupe-key");
var result = q.process("consumer", "orders", "workers", delivery -> {
    // 业务副作用——重复投递时此处不会再次执行
    return handle(delivery.getBody());
});
// result.getStatus(): APPLIED / ALREADY_PROCESSED / NO_MESSAGE / FAILED
```

### HTTP 使用（file 后端）

```properties
queue.backend=file
queue.data-dir=./target/queue-data
```

```bash
TOKEN=admin-token
curl -s -XPOST localhost:8080/api/queue/topics/orders \
  -H "X-Queue-Token: $TOKEN" -H 'Content-Type: application/json' \
  -d '{"capacity":1000,"backpressureStrategy":"WAIT","maxAttempts":3}'
curl -s -XPOST localhost:8080/api/queue/topics/orders/groups/workers \
  -H "X-Queue-Token: $TOKEN"
curl -s -XPOST localhost:8080/api/queue/topics/orders/messages \
  -H "X-Queue-Token: $TOKEN" -H 'Content-Type: application/json' \
  -d '{"body":"hello","producerKey":"k-1"}'
curl -s -XPOST localhost:8080/api/queue/topics/orders/groups/workers/poll \
  -H "X-Queue-Token: $TOKEN"
```

## 9. 错误码速查

| 错误码 | HTTP | 含义 |
| --- | --- | --- |
| INVALID_CREDENTIAL | 401 | 缺失/无法识别的凭据 |
| CROSS_TOPIC_ACCESS / PERMISSION_DENIED | 403 | 跨主题访问 / 权限不足 |
| TOPIC_NOT_FOUND / GROUP_NOT_FOUND / DELIVERY_NOT_FOUND | 404 | 资源不存在 |
| TOPIC_ALREADY_EXISTS / GROUP_ALREADY_EXISTS / BAD_REQUEST / UNSUPPORTED_FORMAT | 400 | 已存在/参数错/旧格式 |
| OFFSET_ROLLBACK_REJECTED / CROSS_GROUP_COMMIT_REJECTED / DELIVERY_STALE | 409 | 非法回退/跨组提交/陈旧投递 |
| QUEUE_FULL / BACKPRESSURE_TIMEOUT | 429 | 立即满队 / 等待超时 |
| OPERATION_TIMEOUT | 408 | 操作超时（如等待被中断） |
| BACKEND_UNAVAILABLE / RUNTIME_CLOSED | 503 | 后端不可用 / 运行时已关闭 |
