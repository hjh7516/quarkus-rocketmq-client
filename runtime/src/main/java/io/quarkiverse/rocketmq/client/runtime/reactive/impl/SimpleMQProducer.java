package io.quarkiverse.rocketmq.client.runtime.reactive.impl;

import java.util.Collection;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import io.quarkiverse.rocketmq.client.runtime.reactive.MQProducer;
import io.quarkiverse.rocketmq.client.runtime.reactive.ReactiveMQProducer;
import io.vertx.rocketmq.client.producer.MessageQueueSelector;
import io.vertx.rocketmq.client.producer.SendResult;
import io.vertx.rocketmq.client.producer.TransactionSendResult;

public class SimpleMQProducer implements MQProducer {

    private final ReactiveMQProducer mqProducer;

    public SimpleMQProducer(ReactiveMQProducer mqProducer) {
        this.mqProducer = mqProducer;
    }

    @Override
    public SendResult send(Message msg) {
        return this.mqProducer.send(msg).await().indefinitely();
    }

    @Override
    public SendResult send(Message msg, long timeout) {
        return this.mqProducer.send(msg, timeout).await().indefinitely();
    }

    @Override
    public void sendOneway(Message msg) {
        this.mqProducer.sendOneway(msg).await().indefinitely();
    }

    @Override
    public SendResult send(Message msg, MessageQueue mq) {
        return this.mqProducer.send(msg, mq).await().indefinitely();
    }

    @Override
    public void sendOneway(Message msg, MessageQueue mq) {
        this.mqProducer.sendOneway(msg, mq).await().indefinitely();
    }

    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg) {
        return null;
    }

    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout) {
        return this.mqProducer.send(msg, selector, arg).await().indefinitely();
    }

    @Override
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg) {
        this.mqProducer.sendOneway(msg, selector, arg).await().indefinitely();
    }

    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, Object arg) {
        return this.mqProducer.sendMessageInTransaction(msg, arg).await().indefinitely();
    }

    @Override
    public SendResult send(Collection<Message> msgs) {
        return this.mqProducer.send(msgs).await().indefinitely();
    }

    @Override
    public SendResult send(Collection<Message> msgs, long timeout) {
        return this.mqProducer.send(msgs, timeout).await().indefinitely();
    }

    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue mq) {
        return this.mqProducer.send(msgs, mq).await().indefinitely();
    }

    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue mq, long timeout) {
        return this.mqProducer.send(msgs, mq, timeout).await().indefinitely();
    }

    @Override
    public Message request(Message msg, long timeout) {
        return this.mqProducer.request(msg, timeout).await().indefinitely();
    }

    @Override
    public Message request(Message msg, MessageQueueSelector selector, Object arg, long timeout) {
        return this.mqProducer.request(msg, selector, arg, timeout).await().indefinitely();
    }
}
