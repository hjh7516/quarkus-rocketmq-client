package io.quarkiverse.rocketmq.client.runtime.reactive.impl;

import java.util.Set;

import org.apache.rocketmq.common.message.MessageQueue;

import io.quarkiverse.rocketmq.client.runtime.reactive.MQPullConsumer;
import io.quarkiverse.rocketmq.client.runtime.reactive.ReactiveMQPullConsumer;
import io.vertx.rocketmq.client.consumer.MessageSelector;
import io.vertx.rocketmq.client.consumer.PullResult;

public class SimpleMQPullConsumer implements MQPullConsumer {

    private final ReactiveMQPullConsumer consumer;

    public SimpleMQPullConsumer(ReactiveMQPullConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums) {
        return this.consumer.pull(mq, subExpression, offset, maxNums).await().indefinitely();
    }

    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeout) {
        return this.consumer.pull(mq, subExpression, offset, maxNums, timeout).await().indefinitely();
    }

    @Override
    public PullResult pull(MessageQueue mq, MessageSelector selector, long offset, int maxNums) {
        return this.consumer.pull(mq, selector, offset, maxNums).await().indefinitely();
    }

    @Override
    public PullResult pull(MessageQueue mq, MessageSelector selector, long offset, int maxNums, long timeout) {
        return this.consumer.pull(mq, selector, offset, maxNums, timeout).await().indefinitely();
    }

    @Override
    public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums) {
        return this.consumer.pullBlockIfNotFound(mq, subExpression, offset, maxNums).await().indefinitely();
    }

    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) {
        return this.consumer.fetchSubscribeMessageQueues(topic).await().indefinitely();
    }
}
