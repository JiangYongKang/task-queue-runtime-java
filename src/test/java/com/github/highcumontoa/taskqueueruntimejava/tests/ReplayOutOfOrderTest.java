package com.github.highcumontoa.taskqueueruntimejava.tests;

import com.github.highcumontoa.taskqueueruntimejava.error.ErrorCode;
import com.github.highcumontoa.taskqueueruntimejava.error.QueueException;
import com.github.highcumontoa.taskqueueruntimejava.model.Delivery;
import com.github.highcumontoa.taskqueueruntimejava.model.DeliveryReason;
import com.github.highcumontoa.taskqueueruntimejava.model.TopicConfig;
import com.github.highcumontoa.taskqueueruntimejava.runtime.TaskQueueRuntime;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.ADMIN;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.CONSUMER;
import static com.github.highcumontoa.taskqueueruntimejava.tests.RuntimeTestSupport.PRODUCER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 乱序确认之后显式重放的位点安全测试（本轮缺陷的稳定复现与回归）。
 *
 * <p>覆盖：
 * <ol>
 *   <li>乱序确认（后面的先确认、更早的仍在处理）后从头/定点重放：
 *       重放必须把区间内此前已确认、且位点排在对外水位线之前的消息也重新投递，
 *       不得静默少投；重放结论带实际覆盖区间与重置条数。</li>
 *   <li>重放与并发确认、处理超时重投交错：在途缺口不受重放影响，
 *       其超时重投原因与 REPLAY 可区分，位点不错位、只增不减、无永久跳过。</li>
 *   <li>重放之后进程重启恢复：文件后端恢复后仍能把全部重置消息投递完，
 *       最终位点落在正确位置。</li>
 * </ol>
 */
class ReplayOutOfOrderTest {

    private static final Logger log = LoggerFactory.getLogger(ReplayOutOfOrderTest.class);
    private static final String T = "repro-events";
    private static final String G = "workers";

    private static TopicConfig config() {
        return new TopicConfig(100, null, Duration.ofSeconds(1),
                Duration.ofSeconds(10),
                com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1);
    }

    private RuntimeTestSupport.Env env() {
        var env = RuntimeTestSupport.memoryRuntime();
        env.runtime().createTopic(ADMIN, T, config());
        env.runtime().createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env, T);
        return env;
    }

    /** 生产 n 条并全部拉取，按 offset 顺序返回投递。 */
    private static List<Delivery> produceAndPollAll(TaskQueueRuntime rt, int n) {
        for (int i = 0; i < n; i++) {
            rt.produce(PRODUCER, T, "m" + i, null);
        }
        List<Delivery> ds = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Delivery d = rt.poll(CONSUMER, T, G);
            assertNotNull(d, "第 " + i + " 条消息必须可拉取");
            ds.add(d);
        }
        return ds;
    }

    @Test
    void headReplay_afterOutOfOrderAcks_redeliversEveryAckedMessage() {
        var env = env();
        TaskQueueRuntime rt = env.runtime();
        List<Delivery> deliveries = produceAndPollAll(rt, 4);

        // 乱序确认：0 先确认（对外水位线=0），随后 2、3 确认，1 仍在处理（缺口）。
        rt.commit(CONSUMER, T, G, deliveries.get(0).getDeliveryId());
        rt.commit(CONSUMER, T, G, deliveries.get(2).getDeliveryId());
        rt.commit(CONSUMER, T, G, deliveries.get(3).getDeliveryId());
        assertEquals(0, rt.offsetOf(CONSUMER, T, G).getCommitted());
        log.info("[case1] before replay: watermark=0 gap inflight=1 ackedAhead=2,3");

        // 从头重放：0/2/3 都曾确认过，按需求都必须重新投递。
        var replayInfo = rt.replay(ADMIN, T, G, -1);
        assertTrue(replayInfo.isReplayed(), "成功结论必须显式标记为重放");
        assertEquals(3, replayInfo.getResetCount(),
                "实际重置条数必须可查：0、2、3 共 3 条（缺口 1 在途不重置）");
        assertEquals(3, replayInfo.getReplayHigh(),
                "重放上界是本组最高已确认位点 3，而非对外水位线 0");
        assertEquals(-1, replayInfo.getReplayFrom());
        log.info("[case1] replay info from={} high={} reset={}",
                replayInfo.getReplayFrom(), replayInfo.getReplayHigh(),
                replayInfo.getResetCount());

        // 重放消息可与仍在途的缺口并发投递：按 offset 升序先 0（1 在途被跳过），
        // 随后 2、3；原因必须是 REPLAY。
        Delivery r0 = rt.poll(CONSUMER, T, G);
        assertNotNull(r0);
        assertEquals(0, r0.getOffset());
        assertEquals(DeliveryReason.REPLAY, r0.getReason());
        rt.commit(CONSUMER, T, G, r0.getDeliveryId());
        assertEquals(0, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "缺口 1 未完成，位点停在 0，不越过在途缺口");

        Delivery r2 = rt.poll(CONSUMER, T, G);
        assertNotNull(r2, "offset=2 此前确认过，重放必须重新投递，不得静默跳过");
        assertEquals(2, r2.getOffset());
        assertEquals(DeliveryReason.REPLAY, r2.getReason());
        Delivery r3 = rt.poll(CONSUMER, T, G);
        assertNotNull(r3, "offset=3 此前确认过，重放必须重新投递，不得静默跳过");
        assertEquals(3, r3.getOffset());
        assertEquals(DeliveryReason.REPLAY, r3.getReason());
        rt.commit(CONSUMER, T, G, r2.getDeliveryId());
        rt.commit(CONSUMER, T, G, r3.getDeliveryId());
        assertEquals(0, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "缺口仍在，重放确认 2、3 后对外位点依旧停在 0，不错位");

        // 缺口消息 1 原处理超时：租约过期后重新投递，原因是 VISIBILITY_TIMEOUT，
        // 与重放原因可区分。
        env.clock().advance(Duration.ofSeconds(11));
        int reclaimed = rt.reclaimExpired(T);
        assertEquals(1, reclaimed, "超时回收的只有缺口消息 offset=1");
        Delivery r1 = rt.poll(CONSUMER, T, G);
        assertNotNull(r1);
        assertEquals(1, r1.getOffset());
        assertEquals(DeliveryReason.VISIBILITY_TIMEOUT, r1.getReason());
        rt.commit(CONSUMER, T, G, r1.getDeliveryId());

        assertEquals(3, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "缺口补齐后位点落在正确位置 3，且全部消息都真实重新投递过");
        assertNull(rt.poll(CONSUMER, T, G), "全部重投完成后不应再有消息");
        log.info("[case1] done committed=3 replayRedelivery=0,2,3 timeoutRedelivery=1");
    }

    @Test
    void targetedReplay_insideRange_alsoCoversAckedAheadMessages() {
        var env = env();
        TaskQueueRuntime rt = env.runtime();
        List<Delivery> ds = produceAndPollAll(rt, 5);
        // 顺序确认 0、1，乱序确认 3、4，留缺口 2；水位线=1，最高确认=4。
        rt.commit(CONSUMER, T, G, ds.get(0).getDeliveryId());
        rt.commit(CONSUMER, T, G, ds.get(1).getDeliveryId());
        rt.commit(CONSUMER, T, G, ds.get(3).getDeliveryId());
        rt.commit(CONSUMER, T, G, ds.get(4).getDeliveryId());
        assertEquals(1, rt.offsetOf(CONSUMER, T, G).getCommitted());
        log.info("[case2] before replay: watermark=1 gap inflight=2 ackedAhead=3,4");

        // 定点重放到 0：覆盖 (0, 4] 内已确认的 1、3、4（缺口 2 在途不重置）。
        var info = rt.replay(ADMIN, T, G, 0);
        assertEquals(3, info.getResetCount(), "重置 1、3、4 三条");
        assertEquals(4, info.getReplayHigh());
        // 0 仍是已确认状态，1 被重置为缺口，因此重算后的连续水位线停在 0。
        assertEquals(0, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "重放下界为 0，对外位点回退到 0（位置 1 被重置成缺口）");

        // 1 先重放投递；2 在途；3、4 随后重放投递。
        Map<Long, DeliveryReason> reasons = new HashMap<>();
        Delivery d = rt.poll(CONSUMER, T, G);
        assertEquals(1, d.getOffset());
        reasons.put(d.getOffset(), d.getReason());
        rt.commit(CONSUMER, T, G, d.getDeliveryId());
        d = rt.poll(CONSUMER, T, G);
        assertEquals(3, d.getOffset());
        reasons.put(d.getOffset(), d.getReason());
        rt.commit(CONSUMER, T, G, d.getDeliveryId());
        d = rt.poll(CONSUMER, T, G);
        assertEquals(4, d.getOffset());
        reasons.put(d.getOffset(), d.getReason());
        rt.commit(CONSUMER, T, G, d.getDeliveryId());
        assertEquals(DeliveryReason.REPLAY, reasons.get(1L));
        assertEquals(DeliveryReason.REPLAY, reasons.get(3L));
        assertEquals(DeliveryReason.REPLAY, reasons.get(4L));
        assertEquals(1, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "0 与重放确认的 1 都已终结，水位线推进到 1；缺口 2 未完成，不越过它");
        log.info("[case2] replay(0) redelivered 1,3,4 with reason REPLAY, watermark=1");

        // 缺口 2 超时重投并确认后，位点追到 4，全程无跳过。
        env.clock().advance(Duration.ofSeconds(11));
        assertEquals(1, rt.reclaimExpired(T));
        d = rt.poll(CONSUMER, T, G);
        assertEquals(2, d.getOffset());
        assertEquals(DeliveryReason.VISIBILITY_TIMEOUT, d.getReason());
        rt.commit(CONSUMER, T, G, d.getDeliveryId());
        assertEquals(4, rt.offsetOf(CONSUMER, T, G).getCommitted());
        assertNull(rt.poll(CONSUMER, T, G));
        log.info("[case2] gap closed committed=4 no skip");
    }

    @Test
    void replayInterleavedWithConcurrentCommitAndTimeout_isSafe() {
        var env = env();
        TaskQueueRuntime rt = env.runtime();
        List<Delivery> ds = produceAndPollAll(rt, 4);
        // 0、2、3 已确认，1 在途（缺口）。
        rt.commit(CONSUMER, T, G, ds.get(0).getDeliveryId());
        rt.commit(CONSUMER, T, G, ds.get(2).getDeliveryId());
        rt.commit(CONSUMER, T, G, ds.get(3).getDeliveryId());

        rt.replay(ADMIN, T, G, -1);
        // 重放后立刻再发起一次“无操作重放”：此刻 0、2、3 已重置（未再确认），
        // 1 在途，区间内没有任何已确认消息 -> 明确拒绝，不能返回错位的成功。
        QueueException noOp = assertThrows(QueueException.class,
                () -> rt.replay(ADMIN, T, G, -1));
        assertEquals(ErrorCode.OFFSET_ROLLBACK_REJECTED, noOp.getCode(),
                "区间内无已确认消息的重放必须与成功重放明确区分");
        log.warn("[case3] repeated replay while nothing acked rejected code={}", noOp.getCode());

        // 重放消息 0 重新生效确认；2、3 也确认；1 仍在途，位点始终停在 0。
        List<Long> polledOffsets = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Delivery d = rt.poll(CONSUMER, T, G);
            assertNotNull(d);
            polledOffsets.add(d.getOffset());
            assertEquals(DeliveryReason.REPLAY, d.getReason());
            rt.commit(CONSUMER, T, G, d.getDeliveryId());
        }
        assertEquals(List.of(0L, 2L, 3L), polledOffsets);
        assertEquals(0, rt.offsetOf(CONSUMER, T, G).getCommitted());

        // 缺口超时重投。
        env.clock().advance(Duration.ofSeconds(11));
        assertEquals(1, rt.reclaimExpired(T));
        Delivery gap = rt.poll(CONSUMER, T, G);
        assertEquals(1, gap.getOffset());
        // 重放与超时交错场景下，重放轮结束后再做一次从头重放是允许的：
        // 已确认的 0、2、3 会再次重置，而 1 此刻还在途。
        rt.commit(CONSUMER, T, G, gap.getDeliveryId());
        assertEquals(3, rt.offsetOf(CONSUMER, T, G).getCommitted(), "缺口补齐，位点=3");

        var second = rt.replay(ADMIN, T, G, -1);
        assertEquals(4, second.getResetCount(), "第二轮重放再次覆盖全部 4 条已确认消息");
        int redelivered = 0;
        while (true) {
            Delivery d = rt.poll(CONSUMER, T, G);
            if (d == null) {
                break;
            }
            assertEquals(DeliveryReason.REPLAY, d.getReason());
            rt.commit(CONSUMER, T, G, d.getDeliveryId());
            redelivered++;
        }
        assertEquals(4, redelivered, "第二轮重放必须重新投递 0,1,2,3 全部 4 条");
        assertEquals(3, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "重放只增不减地回到正确终点 3");
        log.info("[case3] second replay redelivered={} final committed=3", redelivered);
    }

    @Test
    void replayThenRestart_fileBackendFinishesEveryRedelivery() throws Exception {
        java.nio.file.Path dir = java.nio.file.Files.createTempDirectory("queue-replay-gap-");
        var backend = new com.github.highcumontoa.taskqueueruntimejava.backend.LocalFileQueueBackend(dir);
        var env1 = RuntimeTestSupport.newRuntime(backend);
        env1.runtime().createTopic(ADMIN, T, config());
        env1.runtime().createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env1, T);
        TaskQueueRuntime rt1 = env1.runtime();

        List<Delivery> ds = produceAndPollAll(rt1, 4);
        // 0、2、3 已确认，1 仍在途。
        rt1.commit(CONSUMER, T, G, ds.get(0).getDeliveryId());
        rt1.commit(CONSUMER, T, G, ds.get(2).getDeliveryId());
        rt1.commit(CONSUMER, T, G, ds.get(3).getDeliveryId());
        var info = rt1.replay(ADMIN, T, G, -1);
        assertEquals(3, info.getResetCount());
        // 关闭进程：在途缺口 1 被安全交还为可投递；重放待投的 0、2、3 状态已持久化。
        env1.runtime().close(Duration.ZERO);
        log.info("[case4] shutdown after replay: committed=0 replayPending=0,2,3 gap=1 returned");

        var backend2 = new com.github.highcumontoa.taskqueueruntimejava.backend.LocalFileQueueBackend(dir);
        backend2.recover();
        var env2 = RuntimeTestSupport.newRuntime(backend2);
        RuntimeTestSupport.grantTopic(env2, T);
        TaskQueueRuntime rt2 = env2.runtime();

        // 恢复后位点为 0，全部 4 条都必须能重新投递：
        // 0、2、3 原因为 REPLAY（重放标记持久化），1 因关闭交还原因为 VISIBILITY_TIMEOUT。
        Map<Long, DeliveryReason> reasons = new HashMap<>();
        List<Long> order = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Delivery d = rt2.poll(CONSUMER, T, G);
            assertNotNull(d, "重启恢复后第 " + (i + 1) + " 次拉取不得为空");
            order.add(d.getOffset());
            reasons.put(d.getOffset(), d.getReason());
            rt2.commit(CONSUMER, T, G, d.getDeliveryId());
        }
        assertEquals(List.of(0L, 1L, 2L, 3L), order, "恢复后按 offset 连续投递，无少投、无跳过");
        assertEquals(DeliveryReason.REPLAY, reasons.get(0L));
        assertEquals(DeliveryReason.VISIBILITY_TIMEOUT, reasons.get(1L));
        assertEquals(DeliveryReason.REPLAY, reasons.get(2L));
        assertEquals(DeliveryReason.REPLAY, reasons.get(3L));
        assertEquals(3, rt2.offsetOf(CONSUMER, T, G).getCommitted(),
                "重放轮经重启恢复后位点正确落在 3");
        assertNull(rt2.poll(CONSUMER, T, G));
        log.info("[case4] recovered and redelivered all 4, final committed=3 reasons={}", reasons);
        rt2.close(Duration.ZERO);
    }

    @Test
    void processTemplate_replayReappliesBusiness_timeoutRedeliveryDoesNot() {
        var env = env();
        TaskQueueRuntime rt = env.runtime();
        for (int i = 0; i < 3; i++) {
            rt.produce(PRODUCER, T, "m" + i, null);
        }
        java.util.concurrent.atomic.AtomicInteger effects =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.function.Function<Delivery, Object> handler =
                d -> { effects.incrementAndGet(); return null; };

        // 用 process 处理 offset=0（生效 1 次并提交），水位线=0。
        var p0 = rt.process(CONSUMER, T, G, handler);
        assertEquals(0, p0.getCommitResult().getOffset());
        assertEquals(TaskQueueRuntime.ProcessResult.Status.APPLIED, p0.getStatus());
        assertEquals(1, effects.get());

        // 手工把 1 拉走但不提交（缺口，模拟慢处理在途），再用 process 处理 2。
        Delivery gap = rt.poll(CONSUMER, T, G);
        assertEquals(1, gap.getOffset());
        var p2 = rt.process(CONSUMER, T, G, handler);
        assertEquals(2, p2.getCommitResult().getOffset());
        assertEquals(TaskQueueRuntime.ProcessResult.Status.APPLIED, p2.getStatus());
        assertEquals(2, effects.get());
        assertEquals(0, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "缺口 1 在途，水位线停在 0");
        log.info("[case5] initial effects=2 offsets done=0,2 gap inflight=1");

        // 显式重放：0、2 重新生效（业务各再跑一次），缺口 1 不受影响。
        rt.replay(ADMIN, T, G, -1);
        var r0 = rt.process(CONSUMER, T, G, handler);
        assertEquals(0, r0.getCommitResult().getOffset());
        assertEquals(TaskQueueRuntime.ProcessResult.Status.APPLIED, r0.getStatus(),
                "重放的 offset=0 必须重新执行业务");
        var r2 = rt.process(CONSUMER, T, G, handler);
        assertEquals(2, r2.getCommitResult().getOffset());
        assertEquals(TaskQueueRuntime.ProcessResult.Status.APPLIED, r2.getStatus(),
                "重放的 offset=2 必须重新执行业务");
        assertEquals(4, effects.get(), "重放使 0、2 各再生效一次");
        log.info("[case5] replay re-applied offsets=0,2 effects=4");

        // 缺口 1 租约超时后重投：这是它第一次成功生效（在途那次从未提交），
        // 只应生效一次——重放没有清除它的占位（它本来就没有 APPLIED 记录）。
        env.clock().advance(Duration.ofSeconds(11));
        assertEquals(1, rt.reclaimExpired(T));
        var r1 = rt.process(CONSUMER, T, G, handler);
        assertEquals(1, r1.getCommitResult().getOffset());
        assertEquals(TaskQueueRuntime.ProcessResult.Status.APPLIED, r1.getStatus());
        assertEquals(5, effects.get(),
                "缺口 1 首次生效：总副作用 5 次（0 两次、2 两次、1 一次）");
        assertEquals(2, rt.offsetOf(CONSUMER, T, G).getCommitted());
        assertNull(rt.poll(CONSUMER, T, G));
        log.info("[case5] gap offset=1 applied once after timeout reclaim, effects=5 committed=2");
    }

    @Test
    void replayBelowRetention_withGap_isRejectedDistinctly_andNormalOffsetQueryIsNotMarkedReplay() {
        // 容量 10：先顺序消费并提交 0..4，再生产 10 条触发终结前缀回收。
        var env = RuntimeTestSupport.memoryRuntime();
        TaskQueueRuntime rt = env.runtime();
        rt.createTopic(ADMIN, T, new TopicConfig(10, null, Duration.ofSeconds(1),
                Duration.ofMinutes(10),
                com.github.highcumontoa.taskqueueruntimejava.model.BackoffConfig.defaults(), 1));
        rt.createGroup(ADMIN, T, G);
        RuntimeTestSupport.grantTopic(env, T);
        for (int i = 0; i < 5; i++) {
            rt.produce(PRODUCER, T, "early-" + i, null);
            Delivery d = rt.poll(CONSUMER, T, G);
            rt.commit(CONSUMER, T, G, d.getDeliveryId());
        }
        for (int i = 0; i < 10; i++) {
            rt.produce(PRODUCER, T, "later-" + i, null);
        }
        assertEquals(5, rt.offsetOf(CONSUMER, T, G).getEarliestRetained());
        // 消费 5、6 但只确认 6（乱序），5 留在处理中：水位线仍停在 4，最高确认=6。
        Delivery d5 = rt.poll(CONSUMER, T, G);
        Delivery d6 = rt.poll(CONSUMER, T, G);
        assertEquals(5, d5.getOffset());
        assertEquals(6, d6.getOffset());
        rt.commit(CONSUMER, T, G, d6.getDeliveryId());
        assertEquals(4, rt.offsetOf(CONSUMER, T, G).getCommitted(),
                "缺口 5 在途，乱序确认 6 后水位线停在 4");
        log.info("[case6] retainedFrom=5 watermark=4 gap inflight=5 ackedAhead=6");

        // 即使存在乱序缺口，落到保留窗口之前的重放依旧被明确拒绝，
        // 不能因为 6 已确认就返回看似成功的结果。
        QueueException ex = assertThrows(QueueException.class,
                () -> rt.replay(ADMIN, T, G, -1));
        assertEquals(ErrorCode.OFFSET_OUT_OF_RETENTION, ex.getCode());
        log.warn("[case6] head replay below retention rejected distinctly code={}", ex.getCode());

        // 从保留边界重放（target=4，紧邻 retainedFrom=5）：覆盖 6，成功且可区分。
        var info = rt.replay(ADMIN, T, G, 4);
        assertEquals(true, info.isReplayed());
        assertEquals(1, info.getResetCount());
        assertEquals(6, info.getReplayHigh());
        // 普通位点查询不带重放标记，二者可明确区分。
        assertEquals(false, rt.offsetOf(CONSUMER, T, G).isReplayed());
        Delivery redelivered = rt.poll(CONSUMER, T, G);
        assertNotNull(redelivered);
        assertEquals(6, redelivered.getOffset());
        assertEquals(DeliveryReason.REPLAY, redelivered.getReason());
        log.info("[case6] in-retention replay(4) reset offset=6 reason=REPLAY");
    }

    @Test
    void replayOneGroup_doesNotReapplyBusinessOrResetStateInOtherGroup() {
        var env = RuntimeTestSupport.memoryRuntime();
        TaskQueueRuntime rt = env.runtime();
        String g2 = "other-workers";
        rt.createTopic(ADMIN, T, config());
        rt.createGroup(ADMIN, T, G);
        rt.createGroup(ADMIN, T, g2);
        RuntimeTestSupport.grantTopic(env, T);
        rt.produce(PRODUCER, T, "only", null);

        java.util.concurrent.atomic.AtomicInteger g1fx =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger g2fx =
                new java.util.concurrent.atomic.AtomicInteger();
        var p1 = rt.process(CONSUMER, T, G, d -> { g1fx.incrementAndGet(); return null; });
        var p2 = rt.process(CONSUMER, T, g2, d -> { g2fx.incrementAndGet(); return null; });
        assertEquals(TaskQueueRuntime.ProcessResult.Status.APPLIED, p1.getStatus());
        assertEquals(TaskQueueRuntime.ProcessResult.Status.APPLIED, p2.getStatus(),
                "不同消费者组对同一消息必须各自独立生效（修复共享幂等键误短路）");
        assertEquals(1, g1fx.get());
        assertEquals(1, g2fx.get());

        // 只重放 G：g2 状态不动、业务不重复；G 的重放业务重新生效。
        rt.replay(ADMIN, T, G, -1);
        var again = rt.process(CONSUMER, T, G, d -> { g1fx.incrementAndGet(); return null; });
        assertEquals(TaskQueueRuntime.ProcessResult.Status.APPLIED, again.getStatus());
        assertEquals(2, g1fx.get(), "被重放组业务再次生效");
        assertEquals(1, g2fx.get(), "未重放组的业务不受任何影响");
        assertNull(rt.poll(CONSUMER, T, g2), "重放 G 不得重置 g2 的投递状态");
        assertEquals(0, rt.offsetOf(CONSUMER, T, g2).getCommitted(),
                "g2 位点保持其已提交位置 0");
        log.info("[case7] replay G re-applied g1fx=2 while g2 untouched g2fx=1");
    }
}
