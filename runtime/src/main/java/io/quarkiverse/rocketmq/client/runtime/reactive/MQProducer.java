package io.quarkiverse.rocketmq.client.runtime.reactive;

import java.util.Collection;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import io.vertx.rocketmq.client.producer.MessageQueueSelector;
import io.vertx.rocketmq.client.producer.SendResult;
import io.vertx.rocketmq.client.producer.TransactionSendResult;

public interface MQProducer {

    SendResult send(final Message msg);

    SendResult send(final Message msg, final long timeout);

    void sendOneway(final Message msg);

    SendResult send(final Message msg, final MessageQueue mq);

    void sendOneway(final Message msg, final MessageQueue mq);

    SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg);

    SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg, final long timeout);

    void sendOneway(final Message msg, final MessageQueueSelector selector, final Object arg);

    TransactionSendResult sendMessageInTransaction(final Message msg, final Object arg);

    SendResult send(final Collection<Message> msgs);

    SendResult send(final Collection<Message> msgs, final long timeout);

    SendResult send(final Collection<Message> msgs, final MessageQueue mq);

    SendResult send(final Collection<Message> msgs, final MessageQueue mq, final long timeout);

    Message request(final Message msg, final long timeout);

    Message request(final Message msg, final MessageQueueSelector selector, final Object arg, long timeout);
}
