package com.github.highcumontoa.taskqueueruntimejava.runtime;

import com.github.highcumontoa.taskqueueruntimejava.model.CommitResult;
import com.github.highcumontoa.taskqueueruntimejava.model.DeadLetterRecord;
import com.github.highcumontoa.taskqueueruntimejava.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.model.OffsetInfo;
import com.github.highcumontoa.taskqueueruntimejava.model.TopicConfig;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

/**
 * 可嵌入的本地任务队列运行时。至少一次投递 + 幂等去重；
 * 可见性超时重投；退避重试与死信；位点持久化与显式重放；
 * 有界背压；主题级权限。
 */
public interface TaskQueueRuntime {

    void createTopic(String token, String topic, TopicConfig config);

    void createGroup(String token, String topic, String group);

    /** 生产消息。producerKey 非空时做生产侧去重，返回既有 messageId。 */
    ProduceReceipt produce(String token, String topic, String body, String producerKey);

    /**
     * 批量生产：按主题配置的批大小分批落库，逐条返回结论。
     * 部分失败（如容量满）不回退同批已成功项；失败项携带稳定错误码。
     * 单次请求条数不得超过 {@link com.github.highcumontoa.taskqueueruntimejava.model.TopicConfig#MAX_BATCH_SIZE}。
     */
    com.github.highcumontoa.taskqueueruntimejava.model.BatchProduceResult produceBatch(
            String token, String topic,
            java.util.List<com.github.highcumontoa.taskqueueruntimejava.model.ProduceItem> items);

    /**
     * 批量确认：按主题配置的批大小分批提交，逐条返回结论。
     * 部分失败（投递陈旧/不存在等）不影响同批其它提交；位点推进遵守连续水位线规则。
     */
    com.github.highcumontoa.taskqueueruntimejava.model.BatchCommitResult commitBatch(
            String token, String topic, String group, java.util.List<String> deliveryIds);

    /** 拉取一条可投递消息；无消息返回 null。 */
    Delivery poll(String token, String topic, String group);

    /** 确认提交，返回 COMMITTED / ALREADY_COMMITTED / DUPLICATE_DELIVERY。 */
    CommitResult commit(String token, String topic, String group, String deliveryId);

    /**
     * 处理消息的幂等模板：登记幂等键 -> 执行业务 -> 成功提交；
     * 业务失败 nack。重复生效会被短路并标记 ALREADY_PROCESSED。
     */
    <R> ProcessResult<R> process(String token, String topic, String group,
                                 Function<Delivery, R> handler);

    /** 否定确认：进入退避重试或死信。 */
    NackResult nack(String token, String topic, String group, String deliveryId,
                    String error, boolean retryable);

    /** 查询位点。 */
    OffsetInfo offsetOf(String token, String topic, String group);

    /**
     * 显式位点重放（管理员权限）：把半开区间 {@code (targetOffset, ackedHigh]} 内
     * 本组已确认（已提交/已死信）的消息全部重置为可投递，其中 ackedHigh 是本组
     * 最高已确认位点（乱序确认下可能超过对外连续水位线）；仍在处理中的缺口消息
     * 不重置。重放后对外位点按连续水位线重算，与实际重新投递严格一致。
     * 返回的 OffsetInfo 带 {@code replayed/replayFrom/replayHigh/resetCount}，
     * 可核对本次实际覆盖区间与重置条数。
     *
     * <p>结论可区分：参数非法（&lt; -1）-> BAD_REQUEST；区间内无确认可重放
     * （目标已到/超过最高确认位点）-> OFFSET_ROLLBACK_REJECTED；
     * 下界落入已回收历史 -> OFFSET_OUT_OF_RETENTION。
     */
    OffsetInfo replay(String token, String topic, String group, long targetOffset);

    /** 查询死信。 */
    List<DeadLetterRecord> deadLetters(String token, String topic);

    /** 驱动一次租约过期/退避到期重投检查（关闭时归还在途消息）。 */
    int reclaimExpired(String topic);

    /**
     * 回收已彻底终结的消息（所有消费者组均已提交、或已进入死信）。
     * 仅回收从保留边界开始的连续终结前缀；回收后位点单调不倒退，
     * 早于保留边界的历史不可再重放（replay 会以 OFFSET_OUT_OF_RETENTION 拒绝）。
     * 生产路径会自动触发同样的回收，本方法用于手动/定时驱动。
     *
     * @return 本次回收的消息条数
     */
    int reclaimFinished(String topic);

    /** 注册凭据。 */
    void registerCredential(com.github.highcumontoa.taskqueueruntimejava.model.Credential credential);

    /** 授权。 */
    void grant(String token, String topic, com.github.highcumontoa.taskqueueruntimejava.model.Permission permission);

    /** 优雅关闭：等待在途处理完成或等待超时后归还消息。 */
    void close(Duration gracefulWait);

    /** 生产回执。 */
    final class ProduceReceipt {
        private final String messageId;
        private final long offset;
        private final boolean duplicate;

        public ProduceReceipt(String messageId, long offset, boolean duplicate) {
            this.messageId = messageId;
            this.offset = offset;
            this.duplicate = duplicate;
        }

        public String getMessageId() { return messageId; }
        public long getOffset() { return offset; }
        public boolean isDuplicate() { return duplicate; }
    }

    /** 处理结果，区分正常生效与重复短路。 */
    final class ProcessResult<R> {
        public enum Status { APPLIED, ALREADY_PROCESSED, NO_MESSAGE, FAILED }

        private final Status status;
        private final R value;
        private final CommitResult commitResult;
        private final String messageId;
        private final int attempt;
        private final com.github.highcumontoa.taskqueueruntimejava.model.DeliveryReason reason;

        public ProcessResult(Status status, R value, CommitResult commitResult,
                             String messageId, int attempt, com.github.highcumontoa.taskqueueruntimejava.model.DeliveryReason reason) {
            this.status = status;
            this.value = value;
            this.commitResult = commitResult;
            this.messageId = messageId;
            this.attempt = attempt;
            this.reason = reason;
        }

        public Status getStatus() { return status; }
        public R getValue() { return value; }
        public CommitResult getCommitResult() { return commitResult; }
        public String getMessageId() { return messageId; }
        public int getAttempt() { return attempt; }
        public com.github.highcumontoa.taskqueueruntimejava.model.DeliveryReason getReason() { return reason; }
    }

    /** nack 结果：重试等待或进入死信。 */
    final class NackResult {
        public enum Status { RETRY_SCHEDULED, DEAD_LETTER }

        private final Status status;
        private final int attempts;
        private final long retryAfterMillis;
        private final DeadLetterRecord deadLetter;

        public NackResult(Status status, int attempts, long retryAfterMillis, DeadLetterRecord deadLetter) {
            this.status = status;
            this.attempts = attempts;
            this.retryAfterMillis = retryAfterMillis;
            this.deadLetter = deadLetter;
        }

        public Status getStatus() { return status; }
        public int getAttempts() { return attempts; }
        public long getRetryAfterMillis() { return retryAfterMillis; }
        public DeadLetterRecord getDeadLetter() { return deadLetter; }
    }
}
