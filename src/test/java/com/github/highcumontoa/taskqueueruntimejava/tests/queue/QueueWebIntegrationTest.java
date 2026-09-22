package com.github.highcumontoa.taskqueueruntimejava.tests.queue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * REST 端到端：主题/分组/凭据/生产/消费/提交/nack/死信；
 * 越权与无效凭据的 HTTP 状态和 errorCode 必须稳定可区分。
 */
@SpringBootTest(properties = {
        "taskqueue.backend=memory",
        "taskqueue.default-topic-max-depth=3",
        "taskqueue.default-visibility-timeout-millis=60000"
})
@AutoConfigureMockMvc
class QueueWebIntegrationTest {

    private static final String ADMIN = "local-admin-token";
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private MockMvc mvc;

    private JsonNode json(MvcResult result) throws Exception {
        return mapper.readTree(result.getResponse().getContentAsString());
    }

    @Test
    void fullFlowOverHttpWithDistinguishableErrors() throws Exception {
        // 建主题 + 分组
        mvc.perform(post("/api/queue/topics/orders")
                        .header("X-Queue-Token", ADMIN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"maxDepth\":3,\"backpressurePolicy\":\"REJECT\"}"))
                .andExpect(status().isOk());
        mvc.perform(post("/api/queue/topics/orders/groups/warehouse")
                        .header("X-Queue-Token", ADMIN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"visibilityTimeoutMillis\":60000,\"maxAttempts\":1}"))
                .andExpect(status().isOk());

        // 签发只对 orders 的 PRODUCE 凭据
        MvcResult credResult = mvc.perform(post("/api/queue/credentials")
                        .header("X-Queue-Token", ADMIN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"permissions\":[\"PRODUCE\"],\"grantedTopics\":[\"orders\"]}"))
                .andExpect(status().isOk()).andReturn();
        String producerToken = json(credResult).get("token").asText();

        // 无 token -> 401 INVALID_CREDENTIAL
        MvcResult unauth = mvc.perform(post("/api/queue/topics/orders/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":\"x\"}"))
                .andExpect(status().isUnauthorized()).andReturn();
        assertEquals("INVALID_CREDENTIAL", json(unauth).get("errorCode").asText());

        // producer 越权建主题 -> 403 PERMISSION_DENIED
        MvcResult forbidden = mvc.perform(post("/api/queue/topics/other")
                        .header("X-Queue-Token", producerToken))
                .andExpect(status().isForbidden()).andReturn();
        assertEquals("PERMISSION_DENIED", json(forbidden).get("errorCode").asText());

        // 正常生产 3 条
        for (int i = 0; i < 3; i++) {
            mvc.perform(post("/api/queue/topics/orders/messages")
                            .header("X-Queue-Token", producerToken)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"payload\":\"msg" + i + "\"}"))
                    .andExpect(status().isOk());
        }
        // 背压：容量 3，再生产 -> 429 QUEUE_FULL
        MvcResult full = mvc.perform(post("/api/queue/topics/orders/messages")
                        .header("X-Queue-Token", producerToken)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":\"overflow\"}"))
                .andExpect(status().isTooManyRequests()).andReturn();
        assertEquals("QUEUE_FULL", json(full).get("errorCode").asText());

        // 管理员接收
        MvcResult receive = mvc.perform(post("/api/queue/topics/orders/groups/warehouse/receive")
                        .header("X-Queue-Token", ADMIN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"consumerId\":\"w1\",\"maxMessages\":1,\"timeoutMillis\":500}"))
                .andExpect(status().isOk()).andReturn();
        JsonNode delivery = json(receive).get("deliveries").get(0);
        String leaseToken = delivery.get("lease").get("deliveryToken").asText();
        long offset = delivery.get("lease").get("offset").asLong();
        assertEquals(0L, offset);

        // nack（maxAttempts=1，首次即耗尽）-> 死信
        MvcResult nack = mvc.perform(post("/api/queue/topics/orders/groups/warehouse/nack")
                        .header("X-Queue-Token", ADMIN)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"deliveryToken\":\"" + leaseToken
                                + "\",\"errorCode\":\"E_FATAL\",\"errorMessage\":\"bad\",\"retryable\":true}"))
                .andExpect(status().isOk()).andReturn();
        assertEquals("DEAD_LETTER", json(nack).get("outcome").asText());

        // 死信可查询
        MvcResult dlq = mvc.perform(get("/api/queue/topics/orders/groups/warehouse/dead-letters")
                        .header("X-Queue-Token", ADMIN))
                .andExpect(status().isOk()).andReturn();
        assertEquals(1, json(dlq).size());
        assertEquals("RETRY_EXHAUSTED", json(dlq).get(0).get("reason").asText());

        // 位点查询：死信也推进位点
        MvcResult off = mvc.perform(get("/api/queue/topics/orders/groups/warehouse/offset")
                        .header("X-Queue-Token", ADMIN))
                .andExpect(status().isOk()).andReturn();
        assertEquals(1L, json(off).get("committedOffset").asLong());
    }
}
