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
package io.vertx.rocketmq.client.impl.consumer;

import java.util.List;
import java.util.Set;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import io.vertx.rocketmq.client.consumer.MessageQueueListener;
import io.vertx.rocketmq.client.impl.factory.MQClientInstance;
import io.vertx.rocketmq.client.store.ReadOffsetType;

public class RebalanceLitePullImpl extends RebalanceImpl {

    private final DefaultLitePullConsumerImpl litePullConsumerImpl;

    public RebalanceLitePullImpl(DefaultLitePullConsumerImpl litePullConsumerImpl) {
        this(null, null, null, null, litePullConsumerImpl);
    }

    public RebalanceLitePullImpl(String consumerGroup, MessageModel messageModel,
            AllocateMessageQueueStrategy allocateMessageQueueStrategy,
            MQClientInstance mQClientFactory, DefaultLitePullConsumerImpl litePullConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.litePullConsumerImpl = litePullConsumerImpl;
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        MessageQueueListener messageQueueListener = this.litePullConsumerImpl.getDefaultLitePullConsumer()
                .getMessageQueueListener();
        if (messageQueueListener != null) {
            try {
                messageQueueListener.messageQueueChanged(topic, mqAll, mqDivided);
            } catch (Throwable e) {
                log.error("messageQueueChanged exception", e);
            }
        }
    }

    @Override
    public Future<Boolean> removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        Promise<Boolean> promise = Promise.promise();
        this.litePullConsumerImpl.getOffsetStore().persist(mq).onFailure(promise::fail).onSuccess(v -> {
            this.litePullConsumerImpl.getOffsetStore().removeOffset(mq);
            promise.complete(Boolean.TRUE);
        });

        return promise.future();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_ACTIVELY;
    }

    @Override
    public void removeDirtyOffset(final MessageQueue mq) {
        this.litePullConsumerImpl.getOffsetStore().removeOffset(mq);
    }

    @Override
    public Future<Long> computePullFromWhere(MessageQueue mq) {
        ConsumeFromWhere consumeFromWhere = litePullConsumerImpl.getDefaultLitePullConsumer().getConsumeFromWhere();
        Future<Long> offsetFuture;
        switch (consumeFromWhere) {
            case CONSUME_FROM_LAST_OFFSET: {
                offsetFuture = litePullConsumerImpl.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
                offsetFuture.compose(lastOffset -> {
                    Promise<Object> innerPromise = Promise.promise();
                    if (lastOffset >= 0) {
                        innerPromise.complete(lastOffset);
                    } else if (-1 == lastOffset) {
                        if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) { // First start, no offset
                            innerPromise.complete(0L);
                        } else {
                            this.mQClientFactory.getMQAdminImpl().maxOffset(mq).onFailure(e -> innerPromise.complete(-1L))
                                    .onSuccess(innerPromise::complete);
                        }
                    } else {
                        innerPromise.complete(-1L);
                    }
                    return innerPromise.future();
                });

                break;
            }
            case CONSUME_FROM_FIRST_OFFSET: {
                offsetFuture = litePullConsumerImpl.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
                offsetFuture.compose(lastOffset -> {
                    Promise<Object> innerPromise = Promise.promise();
                    if (lastOffset >= 0) {
                        innerPromise.complete(lastOffset);
                    } else if (-1 == lastOffset) {
                        innerPromise.complete(0L);
                    } else {
                        innerPromise.complete(-1L);
                    }
                    return innerPromise.future();
                });

                break;
            }
            case CONSUME_FROM_TIMESTAMP: {
                offsetFuture = litePullConsumerImpl.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
                offsetFuture.compose(lastOffset -> {
                    Promise<Object> innerPromise = Promise.promise();
                    if (lastOffset >= 0) {
                        innerPromise.complete(lastOffset);
                    } else if (-1 == lastOffset) {
                        if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                            this.mQClientFactory.getMQAdminImpl().maxOffset(mq).onFailure(e -> innerPromise.complete(-1L))
                                    .onSuccess(innerPromise::complete);
                        } else {
                            long timestamp = UtilAll
                                    .parseDate(this.litePullConsumerImpl.getDefaultLitePullConsumer().getConsumeTimestamp(),
                                            UtilAll.YYYYMMDDHHMMSS)
                                    .getTime();
                            this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp)
                                    .onFailure(e -> innerPromise.complete(-1L)).onSuccess(innerPromise::complete);
                        }
                    } else {
                        innerPromise.complete(-1L);
                    }
                    return innerPromise.future();
                });
                break;
            }
            default:
                Promise<Long> innerPromise = Promise.promise();
                innerPromise.complete(-1L);
                offsetFuture = innerPromise.future();
        }
        return offsetFuture;
    }

    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequestList) {
    }

}
