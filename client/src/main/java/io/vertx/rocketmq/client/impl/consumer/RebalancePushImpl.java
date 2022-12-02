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
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import io.vertx.rocketmq.client.exception.MQClientException;
import io.vertx.rocketmq.client.impl.factory.MQClientInstance;
import io.vertx.rocketmq.client.store.OffsetStore;
import io.vertx.rocketmq.client.store.ReadOffsetType;

public class RebalancePushImpl extends RebalanceImpl {
    private final static long UNLOCK_DELAY_TIME_MILLS = Long
            .parseLong(System.getProperty("rocketmq.client.unlockDelayTimeMills", "20000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    public RebalancePushImpl(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        this(null, null, null, null, defaultMQPushConsumerImpl);
    }

    public RebalancePushImpl(String consumerGroup, MessageModel messageModel,
            AllocateMessageQueueStrategy allocateMessageQueueStrategy,
            MQClientInstance mQClientFactory, DefaultMQPushConsumerImpl defaultMQPushConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        /**
         * When rebalance result changed, should update subscription's version to notify broker.
         * Fix: inconsistency subscription may lead to consumer miss messages.
         */
        SubscriptionData subscriptionData = this.subscriptionInner.get(topic);
        long newVersion = System.currentTimeMillis();
        log.info("{} Rebalance changed, also update version: {}, {}", topic, subscriptionData.getSubVersion(), newVersion);
        subscriptionData.setSubVersion(newVersion);

        int currentQueueCount = this.processQueueTable.size();
        if (currentQueueCount != 0) {
            int pullThresholdForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForTopic();
            if (pullThresholdForTopic != -1) {
                int newVal = Math.max(1, pullThresholdForTopic / currentQueueCount);
                log.info("The pullThresholdForQueue is changed from {} to {}",
                        this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdForQueue(), newVal);
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdForQueue(newVal);
            }

            int pullThresholdSizeForTopic = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer()
                    .getPullThresholdSizeForTopic();
            if (pullThresholdSizeForTopic != -1) {
                int newVal = Math.max(1, pullThresholdSizeForTopic / currentQueueCount);
                log.info("The pullThresholdSizeForQueue is changed from {} to {}",
                        this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getPullThresholdSizeForQueue(), newVal);
                this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().setPullThresholdSizeForQueue(newVal);
            }
        }

        // notify broker
        this.getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
    }

    @Override
    public Future<Boolean> removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        Promise<Boolean> promise = Promise.promise();
        Future<Void> voidFuture = this.defaultMQPushConsumerImpl.getOffsetStore().persist(mq);
        voidFuture.onFailure(promise::fail);
        voidFuture.onSuccess(v -> {
            this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
            if (this.defaultMQPushConsumerImpl.isConsumeOrderly()
                    && MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
                this.unlockDelay(mq, pq).onComplete(ar -> {
                    if (ar.failed()) {
                        log.error("removeUnnecessaryMessageQueue Exception", ar.cause());
                        promise.complete(false);
                    } else {
                        promise.complete(ar.result());
                    }
                });
            } else {
                promise.complete(true);
            }
        });

        return promise.future();
    }

    private Future<Boolean> unlockDelay(final MessageQueue mq, final ProcessQueue pq) {

        Promise<Boolean> promise = Promise.promise();
        if (pq.hasTempMessage()) {
            log.info("[{}]unlockDelay, begin {} ", mq.hashCode(), mq);
            this.defaultMQPushConsumerImpl.getmQClientFactory().getScheduledExecutorService().schedule(() -> {
                log.info("[{}]unlockDelay, execute at once {}", mq.hashCode(), mq);
                RebalancePushImpl.this.unlock(mq, true);
            }, UNLOCK_DELAY_TIME_MILLS, TimeUnit.MILLISECONDS);
            promise.complete(true);
        } else {
            Future<Void> unlock = this.unlock(mq, true);
            unlock.onFailure(promise::fail);
            unlock.onSuccess(v -> promise.complete(true));
        }
        return promise.future();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public void removeDirtyOffset(final MessageQueue mq) {
        this.defaultMQPushConsumerImpl.getOffsetStore().removeOffset(mq);
    }

    @Override
    public Future<Long> computePullFromWhere(MessageQueue mq) {
        Promise<Long> promise = Promise.promise();
        final ConsumeFromWhere consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer()
                .getConsumeFromWhere();
        final OffsetStore offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
        switch (consumeFromWhere) {
            case CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST:
            case CONSUME_FROM_MIN_OFFSET:
            case CONSUME_FROM_MAX_OFFSET:
            case CONSUME_FROM_LAST_OFFSET: {
                Future<Long> lastOffsetFuture = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                lastOffsetFuture.onFailure(promise::fail);
                lastOffsetFuture.onSuccess(lastOffset -> {
                    if (lastOffset >= 0) {
                        promise.complete(lastOffset);
                    } else if (-1 == lastOffset) {
                        if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                            promise.complete(0L);
                        } else {
                            Future<Long> future = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                            future.onFailure(e -> {
                                if (e instanceof MQClientException) {
                                    promise.complete(-1L);
                                } else {
                                    promise.fail(e);
                                }
                            });
                            future.onSuccess(promise::complete);
                        }
                    } else {
                        promise.complete(-1L);
                    }
                });
                break;
            }
            case CONSUME_FROM_FIRST_OFFSET: {
                Future<Long> lastOffsetFuture = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                lastOffsetFuture.onFailure(promise::fail);
                lastOffsetFuture.onSuccess(lastOffset -> {
                    long result;
                    if (lastOffset >= 0) {
                        result = lastOffset;
                    } else if (-1 == lastOffset) {
                        result = 0L;
                    } else {
                        result = -1;
                    }
                    promise.complete(result);
                });
                break;
            }
            case CONSUME_FROM_TIMESTAMP: {
                Future<Long> lastOffsetFuture = offsetStore.readOffset(mq, ReadOffsetType.READ_FROM_STORE);
                lastOffsetFuture.onFailure(promise::fail);
                lastOffsetFuture.onSuccess(lastOffset -> {
                    if (lastOffset >= 0) {
                        promise.complete(lastOffset);
                    } else if (-1 == lastOffset) {
                        if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                            Future<Long> maxOffsetFuture = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                            maxOffsetFuture.onFailure(e -> {
                                if (e instanceof MQClientException) {
                                    promise.complete(-1L);
                                } else {
                                    promise.fail(e);
                                }
                            });
                            maxOffsetFuture.onSuccess(promise::complete);
                        } else {
                            long timestamp = UtilAll
                                    .parseDate(this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeTimestamp(),
                                            UtilAll.YYYYMMDDHHMMSS)
                                    .getTime();
                            Future<Long> offsetFuture = this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
                            offsetFuture.onFailure(e -> {
                                if (e instanceof MQClientException) {
                                    promise.complete(-1L);
                                } else {
                                    promise.fail(e);
                                }
                            });
                            offsetFuture.onSuccess(promise::complete);
                        }
                    } else {
                        promise.complete(-1L);
                    }
                });
                break;
            }

            default:
                promise.complete(-1L);
                break;
        }

        return promise.future();
    }

    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequestList) {
        for (PullRequest pullRequest : pullRequestList) {
            this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest);
            log.info("doRebalance, {}, add a new pull request {}", consumerGroup, pullRequest);
        }
    }

    @Override
    public Future<Void> dispatchPullRequest(PullRequest pullRequest) {
        return this.defaultMQPushConsumerImpl.executePullRequestImmediately(pullRequest)
                .onSuccess(v -> log.info("doRebalance, {}, add a new pull request {}", consumerGroup, pullRequest));
    }
}
