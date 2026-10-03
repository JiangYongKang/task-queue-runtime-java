package com.github.highcumontoa.taskqueueruntimejava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * REST 冒烟：通过 HTTP 验证创建主题/组、生产、消费、提交、越权拒绝与稳定错误码。
 * 使用配置中内置的 admin-token 引导，生产/消费凭据通过测试内显式注册不涉及，
 * 这里用 ADMIN 全局权限演示最小闭环。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RestApiTest {

    private static final String T = "rest-topic";
    private static final String G = "rest-group";

    @Autowired
    private TestRestTemplate rest;

    private HttpHeaders headers(String token) {
        HttpHeaders h = new HttpHeaders();
        h.set("X-Queue-Token", token);
        h.set("Content-Type", "application/json");
        return h;
    }

    @Test
    void fullLifecycleOverHttp_andErrorCodesAreStable() {
        // 无凭据 -> 401 INVALID_CREDENTIAL
        HttpHeaders noAuth = new HttpHeaders();
        noAuth.set("Content-Type", "application/json");
        ResponseEntity<String> unauth = rest.postForEntity(
                "/api/queue/topics/" + T, new HttpEntity<>("{}", noAuth), String.class);
        assertEquals(HttpStatus.UNAUTHORIZED, unauth.getStatusCode());
        assertTrue(unauth.getBody().contains("INVALID_CREDENTIAL"));

        // 创建主题与组
        ResponseEntity<String> topic = rest.postForEntity(
                "/api/queue/topics/" + T,
                new HttpEntity<>("{\"capacity\":2,\"visibilityTimeoutMillis\":1000}",
                        headers("admin-token")),
                String.class);
        assertEquals(HttpStatus.OK, topic.getStatusCode(), topic.getBody());
        ResponseEntity<String> group = rest.postForEntity(
                "/api/queue/topics/" + T + "/groups/" + G,
                new HttpEntity<>("", headers("admin-token")), String.class);
        assertEquals(HttpStatus.OK, group.getStatusCode());

        // 生产
        var produce = rest.postForEntity(
                "/api/queue/topics/" + T + "/messages",
                new HttpEntity<>("{\"body\":\"hello-http\",\"producerKey\":\"pk-1\"}",
                        headers("admin-token")),
                String.class);
        assertEquals(HttpStatus.OK, produce.getStatusCode());
        assertTrue(produce.getBody().contains("\"duplicate\":false"));
        var dup = rest.postForEntity(
                "/api/queue/topics/" + T + "/messages",
                new HttpEntity<>("{\"body\":\"hello-http\",\"producerKey\":\"pk-1\"}",
                        headers("admin-token")),
                String.class);
        assertTrue(dup.getBody().contains("\"duplicate\":true"), "重复生产可区分");

        // 拉取 + 提交
        @SuppressWarnings("unchecked")
        Map<String, Object> delivery = rest.postForEntity(
                "/api/queue/topics/" + T + "/groups/" + G + "/poll",
                new HttpEntity<>("", headers("admin-token")), Map.class).getBody();
        assertNotNull(delivery);
        String deliveryId = (String) delivery.get("deliveryId");
        assertNotNull(deliveryId);
        var commitResp = rest.postForEntity(
                "/api/queue/topics/" + T + "/groups/" + G + "/commit",
                new HttpEntity<>("{\"deliveryId\":\"" + deliveryId + "\"}",
                        headers("admin-token")),
                String.class);
        assertTrue(commitResp.getBody().contains("COMMITTED"));

        // 重复提交 -> ALREADY_COMMITTED（200，幂等但结论可区分）
        var again = rest.postForEntity(
                "/api/queue/topics/" + T + "/groups/" + G + "/commit",
                new HttpEntity<>("{\"deliveryId\":\"" + deliveryId + "\"}",
                        headers("admin-token")),
                String.class);
        assertTrue(again.getBody().contains("ALREADY_COMMITTED"));
    }

    @Test
    void batchEndpointsOverHttp_partialResultsDistinguishable() {
        String topic = "rest-batch";
        String group = "rest-batch-g";
        // BATCH 策略 + 容量 2 + 批上限 10
        ResponseEntity<String> topicResp = rest.postForEntity(
                "/api/queue/topics/" + topic,
                new HttpEntity<>("{\"capacity\":2,\"backpressureStrategy\":\"BATCH\","
                                + "\"maxBatchSize\":10}", headers("admin-token")),
                String.class);
        assertEquals(HttpStatus.OK, topicResp.getStatusCode(), topicResp.getBody());
        rest.postForEntity("/api/queue/topics/" + topic + "/groups/" + group,
                new HttpEntity<>("", headers("admin-token")), String.class);

        // 批量生产 3 条，容量 2：2 收 1 拒，逐条可区分
        var batchProduce = rest.postForEntity(
                "/api/queue/topics/" + topic + "/messages:batch",
                new HttpEntity<>("{\"items\":[{\"body\":\"b0\"},{\"body\":\"b1\"},"
                                + "{\"body\":\"b2\"}]}", headers("admin-token")),
                String.class);
        assertEquals(HttpStatus.OK, batchProduce.getStatusCode(), batchProduce.getBody());
        assertTrue(batchProduce.getBody().contains("\"acceptedCount\":2"),
                batchProduce.getBody());
        assertTrue(batchProduce.getBody().contains("\"rejectedCount\":1"),
                batchProduce.getBody());
        assertTrue(batchProduce.getBody().contains("QUEUE_FULL"),
                "被拒条目必须带可区分错误码: " + batchProduce.getBody());

        // 拉取 2 条并批量提交（混入一个无效投递）：部分失败可区分
        java.util.List<String> deliveryIds = new java.util.ArrayList<>();
        for (int i = 0; i < 2; i++) {
            @SuppressWarnings("unchecked")
            Map<String, Object> d = rest.postForEntity(
                    "/api/queue/topics/" + topic + "/groups/" + group + "/poll",
                    new HttpEntity<>("", headers("admin-token")), Map.class).getBody();
            assertNotNull(d);
            deliveryIds.add((String) d.get("deliveryId"));
        }
        String commitBody = "{\"deliveryIds\":[\"" + deliveryIds.get(0)
                + "\",\"bogus\",\"" + deliveryIds.get(1) + "\"]}";
        var batchCommit = rest.postForEntity(
                "/api/queue/topics/" + topic + "/groups/" + group + "/commit:batch",
                new HttpEntity<>(commitBody, headers("admin-token")), String.class);
        assertEquals(HttpStatus.OK, batchCommit.getStatusCode(), batchCommit.getBody());
        assertTrue(batchCommit.getBody().contains("\"committedCount\":2"),
                batchCommit.getBody());
        assertTrue(batchCommit.getBody().contains("\"failedCount\":1"),
                batchCommit.getBody());
        assertTrue(batchCommit.getBody().contains("DELIVERY_NOT_FOUND"),
                batchCommit.getBody());

        // 位点连续推进到 1
        var offsetWithAuth = rest.exchange(
                "/api/queue/topics/" + topic + "/groups/" + group + "/offset",
                HttpMethod.GET, new HttpEntity<>(headers("admin-token")), String.class);
        assertTrue(offsetWithAuth.getBody().contains("\"committed\":1"),
                offsetWithAuth.getBody());
    }
}
