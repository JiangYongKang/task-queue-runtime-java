# 本地任务队列运行时 — 投递语义与使用说明

一套可**嵌入**的本地任务队列运行时（库），不依赖任何外部消息服务、真实凭据或付费资源。
提供内存后端（`memory`）与本地可恢复后端（`local-file`），两者语义一致。

- 入口类：`TaskQueueRuntime`（线程安全，`AutoCloseable`）
- 后端工厂：`RuntimeFactory`
- Spring Boot 自动装配：`TaskQueueAutoConfiguration`（Bean：`taskQueueRuntime`）
- REST 薄封装：`/api/queue/**`（请求头 `X-Queue-Token`）

---

## 1. 投递语义（至少一次 + 幂等）

- 同一（主题, 消费者组）下，一条消息**任一时刻最多一个在途租约（lease）**：
  被某消费者取走后，在可见性超时前或提交前不会发给同组其他消费者。
- 未提交的消费在**可见性超时（visibility timeout）**到期后自动回收并可重新投递；
  优雅关闭（`close()`）时也会把全部在途消息安全交还，消息不丢失。
- 投递原因通过 `DeliveryReason` 稳定区分：

  | 原因 | 含义 |
  |---|---|
  | `INITIAL` | 首次投递 |
  | `RETRY_AFTER_NACK` | 消费方显式 nack（可重试）后的**合法重试** |
  | `REDISPATCH_AFTER_VISIBILITY_TIMEOUT` | 可见性超时回收后的**合法重试** |
  | `REDISPATCH_AFTER_SHUTDOWN` | 进程优雅关闭交还后的重新投递 |
  | `REPLAY` | 管理员回退位点后的重放 |

### 去重与幂等结论（可区分）

生产侧：
- 生产请求携带相同 `idempotencyKey`：第一次 `ACCEPTED`，其后稳定返回
  `DUPLICATE_PRODUCE` 且给出**相同位点 / messageId**，不产生新消息。

消费侧（`commit` 的 `CommitOutcome`）：
- `COMMITTED`：业务在该位点首次生效。
- `ALREADY_COMMITTED`：同一投递令牌重复提交，幂等成功，无新副作用。
- `DUPLICATE_DELIVERY`：同一**业务幂等键**此前已在另一位点生效，本次为重复投递，
  被压制、不重复变更业务状态，但位点仍正常前进。

旧投递令牌（例如可见性超时后已重投）再提交，返回稳定错误
`UNKNOWN_DELIVERY_TOKEN`，与上述成功结论区分。

---

## 2. 重试与死信规则

分组级 `RetryPolicy`：`maxAttempts`（含首次投递）、`baseDelayMillis`、
`multiplier`、`maxDelayMillis`。第 n 次重试延迟：

```
delay(n) = min(baseDelayMillis * multiplier^(n-1), maxDelayMillis)
```

- `nack(..., retryable=true)`：未达上限时按退避重新可见，回执
  `RETRY_SCHEDULED`（含 `nextAttempt`、`nextVisibleAtMillis`）。
- 达到 `maxAttempts` 仍失败：进入死信，原因 `RETRY_EXHAUSTED`。
- `nack(..., retryable=false)`：**不重试**，立即进入死信，原因 `NON_RETRYABLE`。
- 死信是**终态**，可通过 `deadLetters(topic, group)` 或 REST
  `GET /api/queue/topics/{t}/groups/{g}/dead-letters` 查询，记录含
  messageId、offset、attempts、reason、errorCode、errorMessage、deadAtMillis。
- 死信同样推动位点连续前进，**不会被静默丢弃，也不会无限重试**。

---

## 3. 位点（offset）与重放管理

- 位点按分组维护：`committedOffset` 之前的所有位点均已处于终态（提交或死信）。
  位点仅在终态连续时向前推进，**不跳变、不越位**。
- 内存后端位点存活于进程内；`local-file` 后端将消息日志与分组状态持久化到磁盘，
  进程重启后位点、死信、在途租约、退避时间全部恢复。
- 管理员回退（`resetOffset`）只允许**回退**（目标必须严格小于当前
  committedOffset 且在日志保留范围内），回退后范围内的消息以 `REPLAY` 重新投递。

非法操作与原因码：

| 场景 | ErrorCode |
|---|---|
| 用历史/旧令牌提交一个已位于 committedOffset 之前的位点 | `OFFSET_ROLLBACK_REJECTED` |
| A 组的投递令牌提到 B 组（跨分组提交） | `CROSS_GROUP_COMMIT_REJECTED` |
| 回退点之后租约已被重置作废、令牌失效 | `UNKNOWN_DELIVERY_TOKEN` |
| 前进式重置位点 | `ILLEGAL_RESET_TARGET` |
| 重置/读取超出日志范围或已淘汰位点 | `OFFSET_OUT_OF_RANGE` |

- 为支持回退重放，主题可配置 `retentionCount`：位点压缩时额外保留最近 N 条
  已终态消息。早于保留窗口的位点被淘汰后重放会得到 `OFFSET_OUT_OF_RANGE`（明确拒绝，无不确定行为）。

---

## 4. 权限模型（生产侧 / 消费侧）

- 凭据为不透明令牌。内置引导管理员令牌（默认 `local-admin-token`，仅用于本地/测试）。
  管理员可通过 `issueCredential` 或 `POST /api/queue/credentials` 签发新令牌。
- 权限位：`PRODUCE`、`CONSUME`、`ADMIN`。`ADMIN` 隐含全部权限。
- 非管理员只能访问授权主题集合（支持 `"*"` 通配）。

三类拒绝稳定可区分：

| 场景 | ErrorCode | HTTP |
|---|---|---|
| 缺失 / 无法识别的令牌 | `INVALID_CREDENTIAL` | 401 |
| 令牌有效但权限不足 | `PERMISSION_DENIED` | 403 |
| 令牌有效但未授权该主题（越权跨主题） | `CROSS_TOPIC_ACCESS_DENIED` | 403 |

一个主题上的拒绝不影响其他合法主题的正常生产/消费。

---

## 5. 有界队列与背压

主题有 `maxDepth` 上限。生产侧策略 `BackpressurePolicy`：

| 策略 | 行为 | 超限错误 |
|---|---|---|
| `REJECT` | 立即拒绝 | `QUEUE_FULL`（HTTP 429） |
| `DELAY` | 阻塞等待消费腾出容量，支持 `waitMillis` 上限 | `PRODUCE_TIMEOUT` |
| `BATCH` | 多个载荷经 `produceBatch` 合并为一条消息；超 `maxBatch` 拒绝 | `BATCH_REJECTED` |

- 消息日志在提交后按“所有分组都已越过”的水位压缩，内存占用**始终有界**。
- 处理（等待容量）超时：`PRODUCE_TIMEOUT`。
- 后端不可用（`LocalFileStorage.setAvailable(false)` 故障注入）：
  `BACKEND_UNAVAILABLE`（HTTP 503），恢复后自动继续。

---

## 6. 后端替换与格式兼容

- `memory`：`AbstractInMemoryStorage`，纯内存，零配置。
- `local-file`：`LocalFileStorage`，消息 JSONL 追加日志 + 分组状态原子快照，
  目录布局：

  ```
  dataDir/
    principals.json
    topics/<topic>/meta.json
    topics/<topic>/messages.log
    topics/<topic>/groups/<group>.json
  ```

- 消息携带 `formatVersion`（当前 v2）。
  - 旧版 **v1**（无 `contentType`/`headers`/`formatVersion`）在启动恢复时被**识别并迁移**：
    缺失字段补确定默认值（`application/octet-stream`、空 headers）。
  - **高于当前版本**的格式抛出 `UNKNOWN_MESSAGE_FORMAT` 明确拒绝，绝不因缺字段产生不确定行为。

---

## 7. 并发与生命周期

- 运行时内部用单一可重入锁 + 条件变量串行化所有状态变更；租约授予、提交、
  nack、重试/死信迁移、位点推进在同一临界区原子完成，无半更新、无双生效、无位点错乱。
- 后台守护线程周期性回收超时租约。
- `close()` 优雅关闭：拒绝新投递（`SHUTDOWN_IN_PROGRESS` / `RUNTIME_CLOSED`），
  回收全部在途消息（持久化后端下重启以 `REDISPATCH_AFTER_SHUTDOWN` 重新投递）。

---

## 8. 本地验证方法

无需任何外部服务。

```bash
# 运行全部测试（22 个，含重复投递、位点回退、重试耗尽、可见性超时、越权访问、背压等）
mvn test

# 打包
mvn package
```

关键测试与关注点：

| 测试类 | 关注点 |
|---|---|
| `DeliverySemanticsTest` | 重复生产、重复提交、重复投递 vs 合法重试、租约排他 |
| `RetryAndDeadLetterTest` | 指数退避、重试耗尽、不可重试立即死信、位点前进 |
| `OffsetAndRecoveryTest` | 位点持久化、重启恢复、回退/跨组/越界拒绝、重放 |
| `AuthorizationTest` | 无效凭据 / 权限不足 / 越权跨主题三种可区分拒绝 |
| `BackpressureTest` | REJECT / DELAY / BATCH、超时与容量释放 |
| `BackendCompatibilityTest` | 内存与本地后端语义一致、v1 迁移、未来格式拒绝 |
| `ConcurrencyAndLifecycleTest` | 多消费者恰好一次生效、超时竞争、关闭交还 |
| `BackendFaultTest` | 后端不可用稳定错误与恢复 |
| `QueueWebIntegrationTest` | REST 全链路与 HTTP 错误码 |

测试日志统一输出 `messageId / group / offset / attempt / reason`，便于解释每次投递结果。

### 嵌入式用法示例

```java
var props = QueueRuntimeProperties.defaults();
try (var rt = RuntimeFactory.create(props)) {
    rt.createTopic("local-admin-token", "orders", null);
    rt.createGroup("local-admin-token", "orders", "warehouse", null);

    var receipt = rt.produce("local-admin-token",
            new EnqueueRequest("orders", "hello".getBytes(), null, null, "biz-123"));

    var rr = rt.receive("local-admin-token", "orders", "warehouse", "worker-1", 1, 1000);
    var delivery = rr.deliveries().get(0);
    rt.commit("local-admin-token", "orders", "warehouse",
            delivery.lease().deliveryToken(), "biz-123");
}
```
