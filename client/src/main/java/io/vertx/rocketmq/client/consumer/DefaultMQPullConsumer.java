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
package io.vertx.rocketmq.client.consumer;

import java.util.HashSet;
import java.util.Set;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.VertxInternal;
import io.vertx.rocketmq.client.ClientConfig;
import io.vertx.rocketmq.client.QueryResult;
import io.vertx.rocketmq.client.common.RocketmqOptions;
import io.vertx.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import io.vertx.rocketmq.client.exception.MQClientException;
import io.vertx.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import io.vertx.rocketmq.client.store.OffsetStore;

/**
 * Default pulling consumer.
 * This class will be removed in 2022, and a better implementation {@link DefaultLitePullConsumer} is recommend to use
 * in the scenario of actively pulling messages.
 */
@Deprecated
public class DefaultMQPullConsumer extends ClientConfig implements MQPullConsumer {

    protected final transient DefaultMQPullConsumerImpl defaultMQPullConsumerImpl;

    /**
     * Do the same thing for the same Group, the application must be set,and guarantee Globally unique
     */
    private String consumerGroup;
    /**
     * Long polling mode, the Consumer connection max suspend time, it is not recommended to modify
     */
    private long brokerSuspendMaxTimeMillis = 1000 * 20;
    /**
     * Long polling mode, the Consumer connection timeout(must greater than brokerSuspendMaxTimeMillis), it is not
     * recommended to modify
     */
    private long consumerTimeoutMillisWhenSuspend = 1000 * 30;
    /**
     * The socket timeout in milliseconds
     */
    private long consumerPullTimeoutMillis = 1000 * 10;
    /**
     * Consumption pattern,default is clustering
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;
    /**
     * Message queue listener
     */
    private MessageQueueListener messageQueueListener;
    /**
     * Offset Storage
     */
    private OffsetStore offsetStore;
    /**
     * Topic set you want to register
     */
    private Set<String> registerTopics = new HashSet<>();
    /**
     * Queue allocation algorithm
     */
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
    /**
     * Whether the unit of subscription group
     */
    private boolean unitMode = false;

    private int maxReconsumeTimes = 16;

    public DefaultMQPullConsumer(VertxInternal vertx, RocketmqOptions options) {
        this(null, MixAll.DEFAULT_CONSUMER_GROUP, vertx, options);
    }

    public DefaultMQPullConsumer(final String consumerGroup, VertxInternal vertx, RocketmqOptions options) {
        this(null, consumerGroup, vertx, options);
    }

    /**
     * Constructor specifying namespace, consumer group and RPC hook.
     *
     * @param consumerGroup Consumer group.
     */
    public DefaultMQPullConsumer(final String namespace, final String consumerGroup, VertxInternal vertx,
            RocketmqOptions options) {
        this.namespace = namespace;
        this.consumerGroup = consumerGroup;
        defaultMQPullConsumerImpl = new DefaultMQPullConsumerImpl(this, vertx, options);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public Future<Void> createTopic(String key, String newTopic, int queueNum) {
        return createTopic(key, withNamespace(newTopic), queueNum, 0);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public Future<Void> createTopic(String key, String newTopic, int queueNum, int topicSysFlag) {
        return this.defaultMQPullConsumerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public Future<Long> searchOffset(MessageQueue mq, long timestamp) {
        return this.defaultMQPullConsumerImpl.searchOffset(queueWithNamespace(mq), timestamp);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public Future<Long> maxOffset(MessageQueue mq) {
        return this.defaultMQPullConsumerImpl.maxOffset(queueWithNamespace(mq));
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public Future<Long> minOffset(MessageQueue mq) {
        return this.defaultMQPullConsumerImpl.minOffset(queueWithNamespace(mq));
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public Future<Long> earliestMsgStoreTime(MessageQueue mq) {
        return this.defaultMQPullConsumerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public Future<MessageExt> viewMessage(String offsetMsgId) {

        return this.defaultMQPullConsumerImpl.viewMessage(offsetMsgId);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public Future<QueryResult> queryMessage(String topic, String key, int maxNum, long begin, long end) {
        return this.defaultMQPullConsumerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public long getBrokerSuspendMaxTimeMillis() {
        return brokerSuspendMaxTimeMillis;
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    public void setBrokerSuspendMaxTimeMillis(long brokerSuspendMaxTimeMillis) {
        this.brokerSuspendMaxTimeMillis = brokerSuspendMaxTimeMillis;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public long getConsumerPullTimeoutMillis() {
        return consumerPullTimeoutMillis;
    }

    public void setConsumerPullTimeoutMillis(long consumerPullTimeoutMillis) {
        this.consumerPullTimeoutMillis = consumerPullTimeoutMillis;
    }

    public long getConsumerTimeoutMillisWhenSuspend() {
        return consumerTimeoutMillisWhenSuspend;
    }

    public void setConsumerTimeoutMillisWhenSuspend(long consumerTimeoutMillisWhenSuspend) {
        this.consumerTimeoutMillisWhenSuspend = consumerTimeoutMillisWhenSuspend;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public MessageQueueListener getMessageQueueListener() {
        return messageQueueListener;
    }

    public void setMessageQueueListener(MessageQueueListener messageQueueListener) {
        this.messageQueueListener = messageQueueListener;
    }

    public Set<String> getRegisterTopics() {
        return registerTopics;
    }

    public void setRegisterTopics(Set<String> registerTopics) {
        this.registerTopics = withNamespace(registerTopics);
    }

    /**
     * This method will be removed or it's visibility will be changed in a certain version after April 5, 2020, so
     * please do not use this method.
     */
    @Deprecated
    @Override
    public Future<Void> sendMessageBack(MessageExt msg, int delayLevel) {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, null);
    }

    /**
     * This method will be removed or it's visibility will be changed in a certain version after April 5, 2020, so
     * please do not use this method.
     */
    @Deprecated
    @Override
    public Future<Void> sendMessageBack(MessageExt msg, int delayLevel, String brokerName) {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, brokerName);
    }

    @Override
    public Future<Set<MessageQueue>> fetchSubscribeMessageQueues(String topic) {
        return this.defaultMQPullConsumerImpl.fetchSubscribeMessageQueues(withNamespace(topic));
    }

    @Override
    public void start() throws MQClientException {
        this.setConsumerGroup(NamespaceUtil.wrapNamespace(this.getNamespace(), this.consumerGroup));
        this.defaultMQPullConsumerImpl.start();
    }

    @Override
    public void shutdown() {
        this.defaultMQPullConsumerImpl.shutdown();
    }

    @Override
    public void registerMessageQueueListener(String topic, MessageQueueListener listener) {
        this.registerTopics.add(withNamespace(topic));
        if (listener != null) {
            this.messageQueueListener = listener;
        }
    }

    @Override
    public Future<PullResult> pull(MessageQueue mq, String subExpression, long offset, int maxNums) {
        return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums);
    }

    @Override
    public Future<PullResult> pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeout) {
        return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, timeout);
    }

    @Override
    public Future<PullResult> pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums) {
        return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), messageSelector, offset, maxNums);
    }

    @Override
    public Future<PullResult> pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums, long timeout) {
        return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), messageSelector, offset, maxNums, timeout);
    }

    @Override
    public Future<PullResult> pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums) {
        return this.defaultMQPullConsumerImpl.pullBlockIfNotFound(queueWithNamespace(mq), subExpression, offset, maxNums);
    }

    @Override
    public void updateConsumeOffset(MessageQueue mq, long offset) throws MQClientException {
        this.defaultMQPullConsumerImpl.updateConsumeOffset(queueWithNamespace(mq), offset);
    }

    @Override
    public Future<Long> fetchConsumeOffset(MessageQueue mq, boolean fromStore) {
        return this.defaultMQPullConsumerImpl.fetchConsumeOffset(queueWithNamespace(mq), fromStore);
    }

    @Override
    public Set<MessageQueue> fetchMessageQueuesInBalance(String topic) throws MQClientException {
        return this.defaultMQPullConsumerImpl.fetchMessageQueuesInBalance(withNamespace(topic));
    }

    @Override
    public Future<MessageExt> viewMessage(String topic, String uniqKey) {
        Promise<MessageExt> promise = Promise.promise();
        this.viewMessage(uniqKey)
                .onFailure(e -> this.defaultMQPullConsumerImpl.queryMessageByUniqKey(withNamespace(topic), uniqKey)
                        .onFailure(promise::fail).onSuccess(promise::complete))
                .onSuccess(promise::complete);
        return promise.future();
    }

    @Override
    public Future<Void> sendMessageBack(MessageExt msg, int delayLevel, String brokerName, String consumerGroup) {
        msg.setTopic(withNamespace(msg.getTopic()));
        return this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, brokerName, consumerGroup);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    public DefaultMQPullConsumerImpl getDefaultMQPullConsumerImpl() {
        return defaultMQPullConsumerImpl;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public int getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final int maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }
}
