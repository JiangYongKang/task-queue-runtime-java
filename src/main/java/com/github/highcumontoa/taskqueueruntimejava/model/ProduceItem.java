package com.github.highcumontoa.taskqueueruntimejava.model;

/** 批量生产请求中的单条消息：消息体 + 可选的生产者幂等键。 */
public final class ProduceItem {
    private String body;
    private String producerKey;

    public ProduceItem() {
    }

    public ProduceItem(String body, String producerKey) {
        this.body = body;
        this.producerKey = producerKey;
    }

    public String getBody() { return body; }
    public void setBody(String body) { this.body = body; }
    public String getProducerKey() { return producerKey; }
    public void setProducerKey(String producerKey) { this.producerKey = producerKey; }
}
