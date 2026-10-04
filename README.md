# 本地可嵌入任务队列运行时（task-queue-runtime-java）

一个**不依赖任何外部消息服务、真实凭据或付费资源**、可直接嵌入 Spring Boot
应用的本地任务队列运行时。提供至少一次投递、消费幂等、可见性超时重投、
退避重试与死信、位点持久化与显式重放、主题级权限、有界背压，以及
**内存后端与本地可恢复文件后端的一致语义**。

面向真实并发与长跑场景的进一步增强：

- **乱序提交位点安全**：消费位点是“连续水位线”，并发消费下后到的消息先确认
  也不会跳过仍在处理中的更早消息；重启后从最早未完成的消息继续投递。
- **乱序确认下的重放覆盖完整**：显式重放的覆盖上界取本组实际已确认的最大
  位点（含超在水位线之前的已确认消息），从头重放不会漏投任何一条；
  重放后对外进度与实际重新投递一致，且与超时重投、重启恢复组合安全。
- **终结消息回收与保留边界**：被所有消费者组处理完或已进入死信的消息会被
  安全回收，容量上限真实约束长期占用；对已清出保留范围的历史重放会以
  独立错误码明确拒绝。
- **批量策略**：新增 `BATCH` 背压策略与批量生产/批量确认 API，按可配置
  批大小成批处理，部分失败逐条可区分且成功项不回退。

所有验证均可在本地完成：`mvn test`（40 个用例，无需任何外部依赖）。

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
- **连续水位线规则（乱序提交安全）**：并发消费时各消费者处理快慢不同，
  确认到达顺序常与消息顺序不一致。对外可见的已提交位点是“连续水位线”——
  只推进到**所有更早消息都已终结**（已提交或已死信）的位置：
  - 后到的消息先确认时，位点**停在缺口之前**，不跳过仍在处理中的更早消息；
  - 缺口消息随后确认时，位点**一次性越过**此前已确认的连续区段；
  - 进程重启恢复后，从最早那条未完成的消息继续投递，已确认的不重复、
    中间的任何一条都不会被永久跳过。
- **显式重放**（管理员）：`replay(group, targetOffset)` 将
  `(targetOffset, maxCommitted]` 区间内本组已确认的消息重置为可投递并回退位点；
  允许 `-1` 表示从头重放（受保留边界约束，见第 6 节），
  重放消息的投递原因为 `REPLAY`。
- **重放覆盖区间与乱序确认的关系**：上界 `maxCommitted` 取本组**实际已确认的
  最大位点**，而不是对外连续水位线。乱序确认时，某些已确认消息的位点会
  “超在”水位线之前（中间还有未处理完的缺口）；这些消息同样在重放覆盖
  区间内，会被重新投递，**不会因排在对外进度之前而被静默跳过**。
  仍在处理中（在途）的消息不属于重放范围，其在途投递保持有效，
  确认后按连续水位线规则并入进度。
- **重放后进度与实际重新投递一致**：重放把位点回退到 `targetOffset`，
  此后随着重放投递被逐条确认，水位线从该点重新单调推进（只增不减）；
  只有真正被重新投递并确认的消息才会计入进度，不会出现“进度追过去了、
  某条其实没被重新投递”的错位。重放待投状态随文件后端持久化，
  **重放进行到一半重启**，恢复后未完成的重放投递会继续完成。
- **重放与消费幂等**：重放只保证“重新投递”。`process` 模板对已生效消息
  仍短路为 `ALREADY_PROCESSED`，业务副作用不因重放重复生效。
- **非法操作被明确拒绝且原因可区分**：
  - 目标位点超过本组实际已确认的最大位点 → `BAD_REQUEST`（拒绝越位前进）；
  - 目标位点等于该上界（无变化回退）→ `OFFSET_ROLLBACK_REJECTED`（409）；
  - 目标区间包含已被回收的历史 → `OFFSET_OUT_OF_RETENTION`（409）；
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

## 5. 有界队列、背压与批量策略

- 每个主题配置硬性容量上限，按**保留中的全部消息**（含已终结待回收）计数；
  任何情况下不允许无界占用内存。生产路径每次都会先回收终结前缀（见第 6 节），
  因此容量上限约束的是**长期占用**而非某个瞬间的待处理数量。
- 积压时按主题配置的策略处理：
  - `REJECT`（默认）：立即失败，错误码 `QUEUE_FULL`（HTTP 429）；
  - `WAIT`：在 `backpressureTimeout` 窗口内等待容量，期间释放主题锁，
    消费者提交释放容量后生产者被唤醒；超时 → `BACKPRESSURE_TIMEOUT`（429）；
  - `BATCH`：面向批量导入/批量结算场景。单条 `produce` 退化为有界等待
    （同 `WAIT`）；配套批量 API 按可配置批大小成批处理（见下）。
- **批量 API**（任意策略下均可用，`BATCH` 策略语义最契合）：
  - `produceBatch(topic, items)` / REST `POST /topics/{t}/messages:batch`：
    按主题 `batchSize` 分块，每块在主题锁内原子落库；
  - `commitBatch(topic, group, deliveryIds)` / REST
    `POST /topics/{t}/groups/{g}/commits:batch`：按 `batchSize` 分块提交。
- **批大小上限**：`batchSize` 默认 100，硬上限 `TopicConfig.MAX_BATCH_SIZE = 500`；
  配置超过上限会被截断，单次批量请求条数超过上限直接 `BAD_REQUEST`。
- **部分失败语义**：批量结果逐条给出结论，**成功项不因同批失败而回退**，
  失败项可精确定位（原始下标 / deliveryId）并携带稳定错误码：
  - 批量生产：`STORED` / `DUPLICATE`（幂等命中）/ `REJECTED`
    （`QUEUE_FULL` 或 `BACKPRESSURE_TIMEOUT`）；
  - 批量确认：`COMMITTED` / `ALREADY_COMMITTED` / 失败
    （`DELIVERY_STALE`、`DELIVERY_NOT_FOUND`、`CROSS_GROUP_COMMIT_REJECTED` 等）。
- **批量路径的并发与位点安全**：批量确认复用与单条相同的提交核心，
  位点仍按连续水位线推进，乱序确认不跳缺口；重复提交幂等短路，
  不会出现双重生效。
- 其它稳定可区分错误：`OPERATION_TIMEOUT`（408）、
  `BACKEND_UNAVAILABLE`（503，落盘失败）、`RUNTIME_CLOSED`（503）。

## 6. 终结消息回收与保留边界

- **回收对象**：对所有消费者组都已提交（`COMMITTED`）或已进入死信
  （`DEAD`）的消息，即“彻底终结”的消息。
- **回收时机**：每次生产（含批量）在容量检查前自动回收；也可通过
  `reclaimFinished(topic)` 显式/定时驱动。只要生产继续，保留占用就被
  容量上限约束住；长跑高吞吐场景内存与磁盘占用有界。
- **只回收连续终结前缀**：保留区间 `[retainedFromOffset, nextOffset)`
  始终连续。若中间某条仍在处理（乱序提交的缺口），其后的终结消息
  会等缺口补齐后随下一次回收一并清理——回收不会造成保留区间“打洞”。
- **位点单调不倒退**：回收只删除已终结消息，`nextOffset` 与各组
  committedOffset 不回退、offset 不重用；新消息照常生产消费。
- **保留边界与重放**：`OffsetInfo.earliestRetained` 暴露当前最早保留位点。
  `replay(targetOffset)` 要求区间 `(targetOffset, maxCommitted]` 完整落在保留
  范围内，否则以 `OFFSET_OUT_OF_RETENTION`（409）**明确拒绝**——
  与正常重放、无变化回退（`OFFSET_ROLLBACK_REJECTED`）都可区分，
  不会悄悄跳过已回收的历史，也不会返回看似成功实则错位的结果。
- **随消息一并过期的派生数据**（取舍说明，选择“回收即彻底失效”）：
  - 各组投递状态、生产者幂等键随消息清理：生产侧去重保证只覆盖保留窗口，
    窗口外相同 `producerKey` 会作为新消息入库；
  - 死信记录随消息清理：死信可查询窗口等于保留窗口；
  - 回收后新建的消费者组只能看到保留范围内的消息（从保留边界之后开始消费）。

## 7. 后端替换与格式兼容

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

## 8. 并发与生命周期

- 生产、消费、提交、nack、重投、死信迁移全部在每主题互斥锁内原子完成，
  杜绝半更新、双重生效与位点错乱。
- `close(gracefulWait)`：先在宽限期内等待在途消息完成；仍未完成的在途消息
  **安全交还为可投递**（下次以超时重投处理），随后拒绝新操作。
  在途数以持久化状态为准，重启恢复后同样正确。

## 9. 本地验证方法

```bash
# 运行全部测试（无需外部服务）
mvn test

# 仅运行重点场景
mvn test -Dtest=DeliveryAndVisibilityTest   # 重复投递 / 可见性超时 / 幂等
mvn test -Dtest=RetryAndDeadLetterTest      # 退避重试 / 重试耗尽 / 死信
mvn test -Dtest=OffsetAndReplayTest         # 位点回退 / 跨组提交 / 重启恢复
mvn test -Dtest=OutOfOrderCommitTest        # 乱序提交水位线 / 乱序后重启续投 / 缺口重放
mvn test -Dtest=ReplayAfterOutOfOrderCommitTest  # 乱序确认后重放覆盖 / 重放与确认交错 / 重放后重启恢复
mvn test -Dtest=RetentionTest               # 长跑占用有界 / 保留边界 / 死信回收
mvn test -Dtest=BatchStrategyTest           # 批量生产确认 / 部分失败 / 批大小上限
mvn test -Dtest=AccessControlTest           # 越权 / 无效凭据
mvn test -Dtest=BackpressureTest            # 背压 REJECT/WAIT
mvn test -Dtest=ConcurrencyLifecycleTest    # 并发互斥 / 关闭交还
mvn test -Dtest=BackendCompatibilityTest    # 双后端语义一致 / 旧格式拒绝
mvn test -Dtest=RestApiTest                 # HTTP 闭环与稳定错误码
```

每个测试类的日志都会输出所覆盖业务场景的关键结论
（messageId / group / offset / attempt / reason / 位点 / 回收量 / 批量统计），
`mvn test` 输出末尾按类汇总各业务的用例数与通过情况。

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

# 批量生产 / 批量确认（BATCH 策略主题，batchSize 可在创建时配置）
curl -s -XPOST localhost:8080/api/queue/topics/orders/messages:batch \
  -H "X-Queue-Token: $TOKEN" -H 'Content-Type: application/json' \
  -d '{"items":[{"body":"a","producerKey":"k-1"},{"body":"b","producerKey":"k-2"}]}'
curl -s -XPOST localhost:8080/api/queue/topics/orders/groups/workers/commits:batch \
  -H "X-Queue-Token: $TOKEN" -H 'Content-Type: application/json' \
  -d '{"deliveryIds":["<deliveryId-1>","<deliveryId-2>"]}'
```

## 10. 错误码速查

| 错误码 | HTTP | 含义 |
| --- | --- | --- |
| INVALID_CREDENTIAL | 401 | 缺失/无法识别的凭据 |
| CROSS_TOPIC_ACCESS / PERMISSION_DENIED | 403 | 跨主题访问 / 权限不足 |
| TOPIC_NOT_FOUND / GROUP_NOT_FOUND / DELIVERY_NOT_FOUND | 404 | 资源不存在 |
| TOPIC_ALREADY_EXISTS / GROUP_ALREADY_EXISTS / BAD_REQUEST / UNSUPPORTED_FORMAT | 400 | 已存在/参数错/旧格式 |
| OFFSET_ROLLBACK_REJECTED / OFFSET_OUT_OF_RETENTION / CROSS_GROUP_COMMIT_REJECTED / DELIVERY_STALE | 409 | 非法回退/超出保留范围/跨组提交/陈旧投递 |
| QUEUE_FULL / BACKPRESSURE_TIMEOUT | 429 | 立即满队 / 等待超时 |
| OPERATION_TIMEOUT | 408 | 操作超时（如等待被中断） |
| BACKEND_UNAVAILABLE / RUNTIME_CLOSED | 503 | 后端不可用 / 运行时已关闭 |
