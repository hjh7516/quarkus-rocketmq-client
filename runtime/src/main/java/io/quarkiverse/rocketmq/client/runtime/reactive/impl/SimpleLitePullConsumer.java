package io.quarkiverse.rocketmq.client.runtime.reactive.impl;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import io.quarkiverse.rocketmq.client.runtime.reactive.LitePullConsumer;
import io.quarkiverse.rocketmq.client.runtime.reactive.ReactiveLitePullConsumer;
import io.vertx.rocketmq.client.consumer.MessageSelector;

public class SimpleLitePullConsumer implements LitePullConsumer {

    private final ReactiveLitePullConsumer consumer;

    public SimpleLitePullConsumer(ReactiveLitePullConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void subscribe(String topic, String subExpression) {
        this.consumer.subscribe(topic, subExpression).await().indefinitely();
    }

    @Override
    public void subscribe(String topic, MessageSelector selector) {
        this.consumer.subscribe(topic, selector).await().indefinitely();
    }

    @Override
    public void unsubscribe(String topic) {
        this.consumer.unsubscribe(topic).await().indefinitely();
    }

    @Override
    public void assign(Collection<MessageQueue> messageQueues) {
        this.consumer.assign(messageQueues).await().indefinitely();
    }

    @Override
    public List<MessageExt> poll() {
        return this.consumer.poll().await().indefinitely();
    }

    @Override
    public List<MessageExt> poll(long timeout) {
        return this.consumer.poll(timeout).await().indefinitely();
    }

    @Override
    public void seek(MessageQueue messageQueue, long offset) {
        this.consumer.seek(messageQueue, offset).await().indefinitely();
    }

    @Override
    public Set<MessageQueue> fetchMessageQueues(String topic) {
        return this.consumer.fetchMessageQueues(topic).await().indefinitely();
    }

    @Override
    public void commitSync() {
        this.consumer.commitSync().await().indefinitely();
    }

    @Override
    public Long committed(MessageQueue messageQueue) {
        return this.consumer.committed(messageQueue).await().indefinitely();
    }

    @Override
    public void seekToBegin(MessageQueue messageQueue) {
        this.consumer.seekToBegin(messageQueue).await().indefinitely();
    }

    @Override
    public void seekToEnd(MessageQueue messageQueue) {
        this.consumer.seekToEnd(messageQueue).await().indefinitely();
    }
}
