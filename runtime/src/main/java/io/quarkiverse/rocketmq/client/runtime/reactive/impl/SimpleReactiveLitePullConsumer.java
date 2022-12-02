package io.quarkiverse.rocketmq.client.runtime.reactive.impl;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import io.quarkiverse.rocketmq.client.runtime.reactive.ReactiveLitePullConsumer;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.AsyncResultUni;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.rocketmq.client.consumer.LitePullConsumer;
import io.vertx.rocketmq.client.consumer.MessageSelector;

public class SimpleReactiveLitePullConsumer implements ReactiveLitePullConsumer {

    private final LitePullConsumer consumer;

    public SimpleReactiveLitePullConsumer(LitePullConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public Uni<Void> subscribe(String topic, String subExpression) {
        return AsyncResultUni.toUni(handler -> this.consumer.subscribe(topic, subExpression)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<Void> subscribe(String topic, MessageSelector selector) {
        return AsyncResultUni.toUni(handler -> this.consumer.subscribe(topic, selector)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<Void> unsubscribe(String topic) {
        return AsyncResultUni.toUni(handler -> {
            try {
                this.consumer.unsubscribe(topic);
                handler.handle(Future.succeededFuture());
            } catch (Exception e) {
                handler.handle(Future.failedFuture(e));
            }
        });
    }

    @Override
    public Uni<Void> assign(Collection<MessageQueue> messageQueues) {
        return AsyncResultUni.toUni(handler -> {
            try {
                this.consumer.assign(messageQueues);
                handler.handle(Future.succeededFuture());
            } catch (Exception e) {
                handler.handle(Future.failedFuture(e));
            }
        });
    }

    @Override
    public Uni<List<MessageExt>> poll() {
        return AsyncResultUni.toUni(handler -> this.consumer.poll()
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<List<MessageExt>> poll(long timeout) {
        return AsyncResultUni.toUni(handler -> this.consumer.poll(timeout)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<Void> seek(MessageQueue messageQueue, long offset) {
        return AsyncResultUni.toUni(handler -> this.consumer.seek(messageQueue, offset)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<Set<MessageQueue>> fetchMessageQueues(String topic) {
        return AsyncResultUni.toUni(handler -> this.consumer.fetchMessageQueues(topic)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<Void> commitSync() {
        return AsyncResultUni.toUni(handler -> this.consumer.commitSync()
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<Long> committed(MessageQueue messageQueue) {
        return AsyncResultUni.toUni(handler -> this.consumer.committed(messageQueue)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<Void> seekToBegin(MessageQueue messageQueue) {
        return AsyncResultUni.toUni(handler -> this.consumer.seekToBegin(messageQueue)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<Void> seekToEnd(MessageQueue messageQueue) {
        return AsyncResultUni.toUni(handler -> this.consumer.seekToEnd(messageQueue)
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
