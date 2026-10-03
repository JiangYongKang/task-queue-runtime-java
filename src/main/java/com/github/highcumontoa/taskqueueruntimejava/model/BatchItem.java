package com.github.highcumontoa.taskqueueruntimejava.model;

/** 批量生产中的单条请求：消息体 + 可选生产侧幂等键。 */
public final class BatchItem {
    private final String body;
    private final String producerKey;

    public BatchItem(String body, String producerKey) {
        this.body = body;
        this.producerKey = producerKey;
    }

    public String getBody() { return body; }
    public String getProducerKey() { return producerKey; }
}
