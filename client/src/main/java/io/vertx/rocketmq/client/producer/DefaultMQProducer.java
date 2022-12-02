/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.vertx.rocketmq.client.producer;

import java.util.Collection;
import java.util.List;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.VertxInternal;
import io.vertx.rocketmq.client.ClientConfig;
import io.vertx.rocketmq.client.QueryResult;
import io.vertx.rocketmq.client.Validators;
import io.vertx.rocketmq.client.common.RocketmqOptions;
import io.vertx.rocketmq.client.exception.MQClientException;
import io.vertx.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import io.vertx.rocketmq.client.log.ClientLogger;
import io.vertx.rocketmq.client.trace.AsyncTraceDispatcher;
import io.vertx.rocketmq.client.trace.TraceDispatcher;
import io.vertx.rocketmq.client.trace.hook.SendMessageTraceHookImpl;

/**
 * This class is the entry point for applications intending to send messages.
 * </p>
 *
 * It's fine to tune fields which exposes getter/setter methods, but keep in mind, all of them should work well out of
 * box for most scenarios.
 * </p>
 *
 * This class aggregates various <code>send</code> methods to deliver messages to brokers. Each of them has pros and
 * cons; you'd better understand strengths and weakness of them before actually coding.
 * </p>
 *
 * <p>
 * <strong>Thread Safety:</strong> After configuring and starting process, this class can be regarded as thread-safe
 * and used among multiple threads context.
 * </p>
 */
public class DefaultMQProducer extends ClientConfig implements MQProducer {

    /**
     * Wrapping internal implementations for virtually all methods presented in this class.
     */
    protected final transient DefaultMQProducerImpl defaultMQProducerImpl;
    private final InternalLogger log = ClientLogger.getLog();
    /**
     * Producer group conceptually aggregates all producer instances of exactly same role, which is particularly
     * important when transactional messages are involved.
     * </p>
     *
     * For non-transactional messages, it does not matter as long as it's unique per process.
     * </p>
     *
     * See {@linktourl http://rocketmq.apache.org/docs/core-concept/} for more discussion.
     */
    private String producerGroup;

    /**
     * Just for testing or demo program
     */
    private String createTopicKey = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;

    /**
     * Number of queues to create per default topic.
     */
    private volatile int defaultTopicQueueNums = 4;

    /**
     * Timeout for sending messages.
     */
    private int sendMsgTimeout = 3000;

    /**
     * Compress message body threshold, namely, message body larger than 4k will be compressed on default.
     */
    private int compressMsgBodyOverHowmuch = 1024 * 4;

    /**
     * Maximum number of retry to perform internally before claiming sending failure in synchronous mode.
     * </p>
     *
     * This may potentially cause message duplication which is up to application developers to resolve.
     */
    private int retryTimesWhenSendFailed = 2;

    /**
     * Maximum number of retry to perform internally before claiming sending failure in asynchronous mode.
     * </p>
     *
     * This may potentially cause message duplication which is up to application developers to resolve.
     */
    private int retryTimesWhenSendAsyncFailed = 2;

    /**
     * Indicate whether to retry another broker on sending failure internally.
     */
    private boolean retryAnotherBrokerWhenNotStoreOK = false;

    /**
     * Maximum allowed message size in bytes.
     */
    private int maxMessageSize = 1024 * 1024 * 4; // 4M

    /**
     * Interface of asynchronous transfer data
     */
    private TraceDispatcher traceDispatcher = null;

    public DefaultMQProducer(VertxInternal vertx, RocketmqOptions options) {
        this(MixAll.DEFAULT_PRODUCER_GROUP, vertx, options);
    }

    public DefaultMQProducer(final String producerGroup, VertxInternal vertx, RocketmqOptions options) {
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, vertx, options);
    }

    /**
     * Constructor specifying producer group.
     *
     * @param producerGroup Producer group, see the name-sake field.
     *        trace topic name.
     */
    public DefaultMQProducer(final String producerGroup, VertxInternal vertx, RocketmqOptions options, boolean enableMsgTrace,
            String customizedTraceTopic) {
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, vertx, options);
        if (enableMsgTrace) {
            try {
                AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(producerGroup, TraceDispatcher.Type.PRODUCE,
                        customizedTraceTopic, vertx, options);
                dispatcher.setHostProducer(this.getDefaultMQProducerImpl());
                traceDispatcher = dispatcher;
                this.getDefaultMQProducerImpl().registerSendMessageHook(
                        new SendMessageTraceHookImpl(traceDispatcher));
            } catch (Throwable e) {
                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
            }
        }
    }

    /**
     * Start this producer instance.
     * </p>
     *
     * <strong> Much internal initializing procedures are carried out to make this instance prepared, thus, it's a must
     * to invoke this method before sending or querying messages. </strong>
     * </p>
     *
     * @throws MQClientException if there is any unexpected error.
     */
    @Override
    public void start() throws MQClientException {
        this.setProducerGroup(withNamespace(this.producerGroup));
        this.defaultMQProducerImpl.start();
        if (null != traceDispatcher) {
            try {
                traceDispatcher.start(this.getNamesrvAddr(), this.getAccessChannel());
            } catch (MQClientException e) {
                log.warn("trace dispatcher start failed ", e);
            }
        }
    }

    /**
     * This method shuts down this producer instance and releases related resources.
     */
    @Override
    public Future<Void> shutdown() {
        return this.defaultMQProducerImpl.shutdown(true).onSuccess(v -> {
            if (null != traceDispatcher) {
                traceDispatcher.shutdown();
            }
        });

    }

    /**
     * Fetch message queues of topic <code>topic</code>, to which we may send/publish messages.
     *
     * @param topic Topic to fetch.
     * @return List of message queues readily to send messages to
     */
    @Override
    public Future<List<MessageQueue>> fetchPublishMessageQueues(String topic) {
        return this.defaultMQProducerImpl.fetchPublishMessageQueues(withNamespace(topic));
    }

    /**
     * Send message in synchronous mode. This method returns only when the sending procedure totally completes.
     * </p>
     *
     * <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal implementation will retry
     * {@link #retryTimesWhenSendFailed} times before claiming failure. As a result, multiple messages may potentially
     * delivered to broker(s). It's up to the application developers to resolve potential duplication issue.
     *
     * @param msg Message to send.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     *         {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     */
    @Override
    public Future<SendResult> send(Message msg) {
        Promise<SendResult> promise = Promise.promise();
        try {
            Validators.checkMessage(msg, this);
            msg.setTopic(withNamespace(msg.getTopic()));
            this.defaultMQProducerImpl.send(msg).onFailure(promise::fail).onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }

        return promise.future();
    }

    /**
     * Same to {@link #send(Message)} with send timeout specified in addition.
     *
     * @param msg Message to send.
     * @param timeout send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     *         {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     */
    @Override
    public Future<SendResult> send(Message msg, long timeout) {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, timeout);
    }

    /**
     * Similar to <a href="https://en.wikipedia.org/wiki/User_Datagram_Protocol">UDP</a>, this method won't wait for
     * acknowledgement from broker before return. Obviously, it has maximums throughput yet potentials of message loss.
     *
     * @param msg Message to send.
     */
    @Override
    public Future<Void> sendOneway(Message msg) {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.sendOneway(msg);
    }

    /**
     * Same to {@link #send(Message)} with target message queue specified in addition.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     *         {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     */
    @Override
    public Future<SendResult> send(Message msg, MessageQueue mq) {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq));
    }

    /**
     * Same to {@link #send(Message)} with target message queue and send timeout specified.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @param timeout send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     *         {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     */
    @Override
    public Future<SendResult> send(Message msg, MessageQueue mq, long timeout) {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), timeout);
    }

    /**
     * Same to {@link #sendOneway(Message)} with target message queue specified.
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     */
    @Override
    public Future<Void> sendOneway(Message msg, MessageQueue mq) {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.sendOneway(msg, queueWithNamespace(mq));
    }

    /**
     * Same to {@link #send(Message)} with message queue selector specified.
     *
     * @param msg Message to send.
     * @param selector Message queue selector, through which we get target message queue to deliver message to.
     * @param arg Argument to work along with message queue selector.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     *         {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     */
    @Override
    public Future<SendResult> send(Message msg, MessageQueueSelector selector, Object arg) {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, selector, arg);
    }

    /**
     * Same to {@link #send(Message, MessageQueueSelector, Object)} with send timeout specified.
     *
     * @param msg Message to send.
     * @param selector Message queue selector, through which we get target message queue to deliver message to.
     * @param arg Argument to work along with message queue selector.
     * @param timeout Send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     *         {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     */
    @Override
    public Future<SendResult> send(Message msg, MessageQueueSelector selector, Object arg, long timeout) {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.send(msg, selector, arg, timeout);
    }

    /**
     * Send request message in synchronous mode. This method returns only when the consumer consume the request message and
     * reply a message.
     * </p>
     *
     * <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal implementation will retry
     * {@link #retryTimesWhenSendFailed} times before claiming failure. As a result, multiple messages may potentially
     * delivered to broker(s). It's up to the application developers to resolve potential duplication issue.
     *
     * @param msg request message to send
     * @param timeout request timeout
     * @return reply message
     */
    @Override
    public Future<Message> request(final Message msg, final long timeout) {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, timeout);
    }

    /**
     * Same to {@link #request(Message, long)} with message queue selector specified.
     *
     * @param msg request message to send
     * @param selector message queue selector, through which we get target message queue to deliver message to.
     * @param arg argument to work along with message queue selector.
     * @param timeout timeout of request.
     * @return reply message
     */
    @Override
    public Future<Message> request(final Message msg, final MessageQueueSelector selector, final Object arg,
            final long timeout) {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.request(msg, selector, arg, timeout);
    }

    /**
     * Same to {@link #sendOneway(Message)} with message queue selector specified.
     *
     * @param msg Message to send.
     * @param selector Message queue selector, through which to determine target message queue to deliver message
     * @param arg Argument used along with message queue selector.
     */
    @Override
    public Future<Void> sendOneway(Message msg, MessageQueueSelector selector, Object arg) {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQProducerImpl.sendOneway(msg, selector, arg);
    }

    /**
     * This method is used to send transactional messages.
     *
     * @param msg Transactional message to send.
     * @param arg Argument used along with local transaction executor.
     * @return Transaction result.
     */
    @Override
    public Future<TransactionSendResult> sendMessageInTransaction(Message msg, Object arg) {
        Promise<TransactionSendResult> promise = Promise.promise();
        promise.fail(new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class"));
        return promise.future();
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param key accesskey
     * @param newTopic topic name
     * @param queueNum topic's queue number
     */
    @Deprecated
    @Override
    public Future<Void> createTopic(String key, String newTopic, int queueNum) {
        return createTopic(key, withNamespace(newTopic), queueNum, 0);
    }

    /**
     * Create a topic on broker. This method will be removed in a certain version after April 5, 2020, so please do not
     * use this method.
     *
     * @param key accesskey
     * @param newTopic topic name
     * @param queueNum topic's queue number
     * @param topicSysFlag topic system flag
     */
    @Deprecated
    @Override
    public Future<Void> createTopic(String key, String newTopic, int queueNum, int topicSysFlag) {
        return this.defaultMQProducerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
    }

    /**
     * Search consume queue offset of the given time stamp.
     *
     * @param mq Instance of MessageQueue
     * @param timestamp from when in milliseconds.
     * @return Consume queue offset.
     */
    @Override
    public Future<Long> searchOffset(MessageQueue mq, long timestamp) {
        return this.defaultMQProducerImpl.searchOffset(queueWithNamespace(mq), timestamp);
    }

    /**
     * Query maximum offset of the given message queue.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param mq Instance of MessageQueue
     * @return maximum offset of the given consume queue.
     */
    @Deprecated
    @Override
    public Future<Long> maxOffset(MessageQueue mq) {
        return this.defaultMQProducerImpl.maxOffset(queueWithNamespace(mq));
    }

    /**
     * Query minimum offset of the given message queue.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param mq Instance of MessageQueue
     * @return minimum offset of the given message queue.
     */
    @Deprecated
    @Override
    public Future<Long> minOffset(MessageQueue mq) {
        return this.defaultMQProducerImpl.minOffset(queueWithNamespace(mq));
    }

    /**
     * Query earliest message store time.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param mq Instance of MessageQueue
     * @return earliest message store time.
     */
    @Deprecated
    @Override
    public Future<Long> earliestMsgStoreTime(MessageQueue mq) {
        return this.defaultMQProducerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
    }

    /**
     * Query message of the given offset message ID.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param offsetMsgId message id
     * @return Message specified.
     */
    @Deprecated
    @Override
    public Future<MessageExt> viewMessage(String offsetMsgId) {
        return this.defaultMQProducerImpl.viewMessage(offsetMsgId);
    }

    /**
     * Query message by key.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param topic message topic
     * @param key message key index word
     * @param maxNum max message number
     * @param begin from when
     * @param end to when
     * @return QueryResult instance contains matched messages.
     */
    @Deprecated
    @Override
    public Future<QueryResult> queryMessage(String topic, String key, int maxNum, long begin, long end) {
        return this.defaultMQProducerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
    }

    /**
     * Query message of the given message ID.
     *
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     *
     * @param topic Topic
     * @param msgId Message ID
     * @return Message specified.
     */
    @Deprecated
    @Override
    public Future<MessageExt> viewMessage(String topic, String msgId) {
        Promise<MessageExt> promise = Promise.promise();
        this.viewMessage(msgId).onComplete(ar -> {
            if (ar.failed()) {
                this.defaultMQProducerImpl.queryMessageByUniqKey(withNamespace(topic), msgId)
                        .onFailure(promise::fail).onSuccess(promise::complete);
            } else {
                promise.complete(ar.result());
            }
        });
        return promise.future();
    }

    @Override
    public Future<SendResult> send(Collection<Message> msgs) {
        Promise<SendResult> promise = Promise.promise();
        try {
            this.defaultMQProducerImpl.send(batch(msgs)).onFailure(promise::fail).onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }
        return promise.future();
    }

    @Override
    public Future<SendResult> send(Collection<Message> msgs, long timeout) {
        Promise<SendResult> promise = Promise.promise();
        try {
            this.defaultMQProducerImpl.send(batch(msgs), timeout).onFailure(promise::fail).onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }

        return promise.future();
    }

    @Override
    public Future<SendResult> send(Collection<Message> msgs, MessageQueue messageQueue) {
        Promise<SendResult> promise = Promise.promise();
        try {
            this.defaultMQProducerImpl.send(batch(msgs), messageQueue).onFailure(promise::fail).onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }

        return promise.future();
    }

    @Override
    public Future<SendResult> send(Collection<Message> msgs, MessageQueue messageQueue, long timeout) {
        Promise<SendResult> promise = Promise.promise();
        try {
            this.defaultMQProducerImpl.send(batch(msgs), messageQueue, timeout).onFailure(promise::fail)
                    .onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }

        return promise.future();
    }

    private MessageBatch batch(Collection<Message> msgs) throws MQClientException {
        MessageBatch msgBatch;
        try {
            msgBatch = MessageBatch.generateFromList(msgs);
            for (Message message : msgBatch) {
                Validators.checkMessage(message, this);
                MessageClientIDSetter.setUniqID(message);
                message.setTopic(withNamespace(message.getTopic()));
            }
            msgBatch.setBody(msgBatch.encode());
        } catch (Exception e) {
            throw new MQClientException("Failed to initiate the MessageBatch", e);
        }
        msgBatch.setTopic(withNamespace(msgBatch.getTopic()));
        return msgBatch;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getCreateTopicKey() {
        return createTopicKey;
    }

    public void setCreateTopicKey(String createTopicKey) {
        this.createTopicKey = createTopicKey;
    }

    public int getSendMsgTimeout() {
        return sendMsgTimeout;
    }

    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }

    public int getCompressMsgBodyOverHowmuch() {
        return compressMsgBodyOverHowmuch;
    }

    public void setCompressMsgBodyOverHowmuch(int compressMsgBodyOverHowmuch) {
        this.compressMsgBodyOverHowmuch = compressMsgBodyOverHowmuch;
    }

    @Deprecated
    public DefaultMQProducerImpl getDefaultMQProducerImpl() {
        return defaultMQProducerImpl;
    }

    public boolean isRetryAnotherBrokerWhenNotStoreOK() {
        return retryAnotherBrokerWhenNotStoreOK;
    }

    public void setRetryAnotherBrokerWhenNotStoreOK(boolean retryAnotherBrokerWhenNotStoreOK) {
        this.retryAnotherBrokerWhenNotStoreOK = retryAnotherBrokerWhenNotStoreOK;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public boolean isSendMessageWithVIPChannel() {
        return isVipChannelEnabled();
    }

    public void setSendMessageWithVIPChannel(final boolean sendMessageWithVIPChannel) {
        this.setVipChannelEnabled(sendMessageWithVIPChannel);
    }

    public long[] getNotAvailableDuration() {
        return this.defaultMQProducerImpl.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.defaultMQProducerImpl.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.defaultMQProducerImpl.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.defaultMQProducerImpl.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.defaultMQProducerImpl.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.defaultMQProducerImpl.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public int getRetryTimesWhenSendAsyncFailed() {
        return retryTimesWhenSendAsyncFailed;
    }

    public void setRetryTimesWhenSendAsyncFailed(final int retryTimesWhenSendAsyncFailed) {
        this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
    }

    public TraceDispatcher getTraceDispatcher() {
        return traceDispatcher;
    }

}
