package io.quarkiverse.rocketmq.client.runtime.reactive.impl;

import java.util.Collection;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import io.quarkiverse.rocketmq.client.runtime.reactive.ReactiveMQProducer;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.AsyncResultUni;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.rocketmq.client.producer.MQProducer;
import io.vertx.rocketmq.client.producer.MessageQueueSelector;
import io.vertx.rocketmq.client.producer.SendResult;
import io.vertx.rocketmq.client.producer.TransactionSendResult;

public class ReactiveMQProducerImpl implements ReactiveMQProducer {

    private final MQProducer mqProducer;

    public ReactiveMQProducerImpl(MQProducer mqProducer) {
        this.mqProducer = mqProducer;
    }

    @Override
    public Uni<SendResult> send(Message msg) {
        return AsyncResultUni.toUni(handler -> mqProducer.send(msg)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<SendResult> send(Message msg, long timeout) {
        return AsyncResultUni.toUni(handler -> mqProducer.send(msg, timeout)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<Void> sendOneway(Message msg) {
        return AsyncResultUni.toUni(handler -> mqProducer.sendOneway(msg)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<SendResult> send(Message msg, MessageQueue mq) {
        return AsyncResultUni.toUni(handler -> mqProducer.send(msg, mq)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<SendResult> send(Message msg, MessageQueue mq, long timeout) {
        return AsyncResultUni.toUni(handler -> mqProducer.send(msg, mq, timeout)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public SendResult sendAndAwait(Message msg, MessageQueue mq, long timeout) {
        return send(msg, mq, timeout).await().indefinitely();
    }

    @Override
    public Uni<Void> sendOneway(Message msg, MessageQueue mq) {
        return AsyncResultUni.toUni(handler -> mqProducer.sendOneway(msg, mq)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<SendResult> send(Message msg, MessageQueueSelector selector, Object arg) {
        return AsyncResultUni.toUni(handler -> mqProducer.send(msg, selector, arg)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<SendResult> send(Message msg, MessageQueueSelector selector, Object arg, long timeout) {
        return AsyncResultUni.toUni(handler -> mqProducer.send(msg, selector, arg, timeout)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<Void> sendOneway(Message msg, MessageQueueSelector selector, Object arg) {
        return AsyncResultUni.toUni(handler -> mqProducer.sendOneway(msg, selector, arg)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<TransactionSendResult> sendMessageInTransaction(Message msg, Object arg) {
        return AsyncResultUni.toUni(handler -> mqProducer.sendMessageInTransaction(msg, arg)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<SendResult> send(Collection<Message> msgs) {
        return AsyncResultUni.toUni(handler -> mqProducer.send(msgs)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<SendResult> send(Collection<Message> msgs, long timeout) {
        return AsyncResultUni.toUni(handler -> mqProducer.send(msgs, timeout)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<SendResult> send(Collection<Message> msgs, MessageQueue mq) {
        return AsyncResultUni.toUni(handler -> mqProducer.send(msgs, mq)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<SendResult> send(Collection<Message> msgs, MessageQueue mq, long timeout) {
        return AsyncResultUni.toUni(handler -> mqProducer.send(msgs, mq, timeout)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<Message> request(Message msg, long timeout) {
        return AsyncResultUni.toUni(handler -> mqProducer.request(msg, timeout)
                .onComplete(ar -> emit(handler, ar)));
    }

    @Override
    public Uni<Message> request(Message msg, MessageQueueSelector selector, Object arg, long timeout) {
        return AsyncResultUni.toUni(handler -> mqProducer.request(msg, selector, arg, timeout)
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
