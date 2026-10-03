package com.github.highcumontoa.taskqueueruntimejava.runtime;

import com.github.highcumontoa.taskqueueruntimejava.model.BatchCommitResult;
import com.github.highcumontoa.taskqueueruntimejava.model.BatchItem;
import com.github.highcumontoa.taskqueueruntimejava.model.BatchProduceResult;
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
     * 批量生产：逐条结论可区分（ACCEPTED / DUPLICATE / REJECTED）。
     * 部分失败不回退已成功条目；批大小超过 maxBatchSize 整批拒绝（BATCH_TOO_LARGE）。
     */
    BatchProduceResult produceBatch(String token, String topic, List<BatchItem> items);

    /**
     * 批量提交：逐条结论可区分，成功条目不回退；
     * 组位点按连续水位推进，不跳过未完成的更早消息。
     */
    BatchCommitResult commitBatch(String token, String topic, String group, List<String> deliveryIds);

    /**
     * 回收已终结（全组提交/死信）且超过保留期的消息与死信记录。
     * 回收按 offset 前缀推进 baseOffset，位点单调不倒退。
     *
     * @return 回收的消息条数
     */
    int reclaimTerminated(String topic);

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
     * 显式位点重放：将已提交位点回退到 targetOffset（管理员权限）。
     * 普通 commit 回退会被拒绝（OFFSET_ROLLBACK_REJECTED）。
     */
    OffsetInfo replay(String token, String topic, String group, long targetOffset);

    /** 查询死信。 */
    List<DeadLetterRecord> deadLetters(String token, String topic);

    /** 驱动一次租约过期/退避到期重投检查（关闭时归还在途消息）。 */
    int reclaimExpired(String topic);

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
