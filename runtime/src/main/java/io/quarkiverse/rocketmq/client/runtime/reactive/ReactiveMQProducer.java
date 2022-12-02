package io.quarkiverse.rocketmq.client.runtime.reactive;

import java.util.Collection;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import io.smallrye.mutiny.Uni;
import io.vertx.rocketmq.client.producer.MessageQueueSelector;
import io.vertx.rocketmq.client.producer.SendResult;
import io.vertx.rocketmq.client.producer.TransactionSendResult;

public interface ReactiveMQProducer {

    Uni<SendResult> send(final Message msg);

    Uni<SendResult> send(final Message msg, final long timeout);

    Uni<Void> sendOneway(final Message msg);

    Uni<SendResult> send(final Message msg, final MessageQueue mq);

    Uni<SendResult> send(final Message msg, final MessageQueue mq, final long timeout);

    SendResult sendAndAwait(final Message msg, final MessageQueue mq, final long timeout);

    Uni<Void> sendOneway(final Message msg, final MessageQueue mq);

    Uni<SendResult> send(final Message msg, final MessageQueueSelector selector, final Object arg);

    Uni<SendResult> send(final Message msg, final MessageQueueSelector selector, final Object arg, final long timeout);

    Uni<Void> sendOneway(final Message msg, final MessageQueueSelector selector, final Object arg);

    Uni<TransactionSendResult> sendMessageInTransaction(final Message msg, final Object arg);

    //for batch
    Uni<SendResult> send(final Collection<Message> msgs);

    Uni<SendResult> send(final Collection<Message> msgs, final long timeout);

    Uni<SendResult> send(final Collection<Message> msgs, final MessageQueue mq);

    Uni<SendResult> send(final Collection<Message> msgs, final MessageQueue mq, final long timeout);

    //for rpc
    Uni<Message> request(final Message msg, final long timeout);

    Uni<Message> request(final Message msg, final MessageQueueSelector selector, final Object arg, long timeout);

}
