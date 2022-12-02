package io.quarkiverse.rocketmq.client.runtime.reactive.impl;

import java.util.Set;

import org.apache.rocketmq.common.message.MessageQueue;

import io.quarkiverse.rocketmq.client.runtime.reactive.ReactiveMQPullConsumer;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.AsyncResultUni;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.rocketmq.client.consumer.MQPullConsumer;
import io.vertx.rocketmq.client.consumer.MessageSelector;
import io.vertx.rocketmq.client.consumer.PullResult;

public class SimpleReactiveMQPullConsumer implements ReactiveMQPullConsumer {

    private final MQPullConsumer consumer;

    public SimpleReactiveMQPullConsumer(MQPullConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public Uni<PullResult> pull(MessageQueue mq, String subExpression, long offset, int maxNums) {
        return AsyncResultUni.toUni(handler -> this.consumer.pull(mq, subExpression, offset, maxNums)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<PullResult> pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeout) {
        return AsyncResultUni.toUni(handler -> this.consumer.pull(mq, subExpression, offset, maxNums, timeout)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<PullResult> pull(MessageQueue mq, MessageSelector selector, long offset, int maxNums) {
        return AsyncResultUni.toUni(handler -> this.consumer.pull(mq, selector, offset, maxNums)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<PullResult> pull(MessageQueue mq, MessageSelector selector, long offset, int maxNums, long timeout) {
        return AsyncResultUni.toUni(handler -> this.consumer.pull(mq, selector, offset, maxNums, timeout)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<PullResult> pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums) {
        return AsyncResultUni.toUni(handler -> this.consumer.pullBlockIfNotFound(mq, subExpression, offset, maxNums)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<Set<MessageQueue>> fetchSubscribeMessageQueues(String topic) {
        return AsyncResultUni.toUni(handler -> this.consumer.fetchSubscribeMessageQueues(topic)
                .onComplete(ar -> emit(handler, ar)));
    }

    private <T> void emit(Handler<AsyncResult<T>> handler, AsyncResult<T> ar) {
        if (ar.succeeded()) {
            handler.handle(Future.succeededFuture(ar.result()));
        } else {
            handler.handle(Future.failedFuture(ar.cause()));
        }
    }
}
