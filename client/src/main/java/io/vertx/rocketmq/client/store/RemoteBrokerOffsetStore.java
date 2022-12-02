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
package io.vertx.rocketmq.client.store;

import static io.vertx.rocketmq.client.store.ReadOffsetType.READ_FROM_STORE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.rocketmq.client.exception.MQBrokerException;
import io.vertx.rocketmq.client.exception.MQClientException;
import io.vertx.rocketmq.client.impl.FindBrokerResult;
import io.vertx.rocketmq.client.impl.factory.MQClientInstance;
import io.vertx.rocketmq.client.log.ClientLogger;

/**
 * Remote storage implementation
 */
public class RemoteBrokerOffsetStore implements OffsetStore {
    private final static InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private final String groupName;
    private final ConcurrentMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<>();

    public RemoteBrokerOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
    }

    @Override
    public Future<Void> load() {
        Promise<Void> promise = Promise.promise();
        promise.complete();
        return promise.future();
    }

    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            AtomicLong offsetOld = this.offsetTable.get(mq);
            if (null == offsetOld) {
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
            }

            if (null != offsetOld) {
                if (increaseOnly) {
                    MixAll.compareAndIncreaseOnly(offsetOld, offset);
                } else {
                    offsetOld.set(offset);
                }
            }
        }
    }

    @Override
    public Future<Long> readOffset(final MessageQueue mq, final ReadOffsetType type) {
        Promise<Long> promise = Promise.promise();
        if (mq != null) {
            if (READ_FROM_STORE.equals(type)) {
                Future<Long> brokerOffsetFuture = this.fetchConsumeOffsetFromBroker(mq);
                brokerOffsetFuture.onFailure(e -> {
                    if (e instanceof MQBrokerException) {
                        promise.complete(-1L);
                    } else {
                        log.warn("fetchConsumeOffsetFromBroker exception, " + mq, e);
                        promise.complete(-2L);
                    }
                });
                brokerOffsetFuture.onSuccess(brokerOffset -> {
                    this.updateOffset(mq, brokerOffset, false);
                    promise.complete(brokerOffset);
                });
            } else {
                promise.complete(Optional.ofNullable(this.offsetTable.get(mq)).map(AtomicLong::get).orElse(-1L));
            }
        } else {
            promise.complete(-1L);
        }
        return promise.future();
    }

    @Override
    public Future<Void> persistAll(Set<MessageQueue> mqs) {
        Promise<Void> promise = Promise.promise();
        if (null == mqs || mqs.isEmpty()) {
            promise.complete();
            return promise.future();
        }

        final HashSet<MessageQueue> unusedMQ = new HashSet<>();
        List<Future> resultFuturs = new ArrayList<>();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            AtomicLong offset = entry.getValue();
            if (offset != null) {
                if (mqs.contains(mq)) {
                    Future<Void> voidFuture = this.updateConsumeOffsetToBroker(mq, offset.get());
                    voidFuture.onFailure(e -> log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e));
                    voidFuture.onSuccess(v -> log.info("[persistAll] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                            this.groupName,
                            this.mQClientFactory.getClientId(),
                            mq,
                            offset.get()));
                    resultFuturs.add(voidFuture);
                } else {
                    unusedMQ.add(mq);
                }
            }
        }

        if (!unusedMQ.isEmpty()) {
            for (MessageQueue mq : unusedMQ) {
                this.offsetTable.remove(mq);
                log.info("remove unused mq, {}, {}", mq, this.groupName);
            }
        }

        CompositeFuture.any(resultFuturs).onComplete(ar -> promise.complete());
        return promise.future();
    }

    @Override
    public Future<Void> persist(MessageQueue mq) {
        Promise<Void> promise = Promise.promise();
        AtomicLong offset = this.offsetTable.get(mq);
        if (offset != null) {
            Future<Void> voidFuture = this.updateConsumeOffsetToBroker(mq, offset.get());
            voidFuture.onComplete(ar -> {
                if (ar.failed()) {
                    log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), ar.cause());
                } else {
                    log.info("[persist] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                            this.groupName,
                            this.mQClientFactory.getClientId(),
                            mq,
                            offset.get());
                }
                promise.complete();
            });
        } else {
            promise.complete();
        }

        return promise.future();
    }

    public void removeOffset(MessageQueue mq) {
        if (mq != null) {
            this.offsetTable.remove(mq);
            log.info("remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}", this.groupName, mq,
                    offsetTable.size());
        }
    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<>();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, entry.getValue().get());
        }
        return cloneOffsetTable;
    }

    /**
     * Update the Consumer Offset in one way, once the Master is off, updated to Slave, here need to be optimized.
     */
    private Future<Void> updateConsumeOffsetToBroker(MessageQueue mq, long offset) {
        return updateConsumeOffsetToBroker(mq, offset, true);
    }

    /**
     * Update the Consumer Offset synchronously, once the Master is off, updated to Slave, here need to be optimized.
     */
    @Override
    public Future<Void> updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) {

        Promise<Void> promise = Promise.promise();

        Promise<FindBrokerResult> brokerResultPromise = Promise.promise();
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {
            Future<Boolean> booleanFuture = this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            booleanFuture.onFailure(brokerResultPromise::fail);
            booleanFuture.onSuccess(bool -> {
                brokerResultPromise.complete(this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName()));
            });
        } else {
            brokerResultPromise.complete(findBrokerResult);
        }

        brokerResultPromise.future().onFailure(promise::fail);
        brokerResultPromise.future().onSuccess(brokerResult -> {
            if (brokerResult != null) {
                UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
                requestHeader.setTopic(mq.getTopic());
                requestHeader.setConsumerGroup(this.groupName);
                requestHeader.setQueueId(mq.getQueueId());
                requestHeader.setCommitOffset(offset);

                Future<Void> updateResult;
                if (isOneway) {
                    updateResult = this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffsetOneway(
                            brokerResult.getBrokerAddr(), requestHeader);
                } else {
                    updateResult = this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffset(
                            brokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
                }
                updateResult.onFailure(promise::fail).onSuccess(promise::complete);
            } else {
                promise.fail(new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null));
            }
        });

        return promise.future();
    }

    private Future<Long> fetchConsumeOffsetFromBroker(MessageQueue mq) {
        Promise<Long> promise = Promise.promise();
        Promise<FindBrokerResult> brokerResultPromise = Promise.promise();
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {
            Future<Boolean> boolFuture = this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            boolFuture.onFailure(brokerResultPromise::fail);
            boolFuture.onSuccess(
                    bool -> brokerResultPromise.complete(this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName())));
        } else {
            brokerResultPromise.complete(findBrokerResult);
        }

        brokerResultPromise.future().onFailure(promise::fail);
        brokerResultPromise.future().onSuccess(brokerResult -> {
            if (brokerResult != null) {
                QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
                requestHeader.setTopic(mq.getTopic());
                requestHeader.setConsumerGroup(this.groupName);
                requestHeader.setQueueId(mq.getQueueId());

                this.mQClientFactory.getMQClientAPIImpl().queryConsumerOffset(
                        brokerResult.getBrokerAddr(), requestHeader, 1000 * 5)
                        .onFailure(promise::fail)
                        .onSuccess(promise::complete);
            } else {
                promise.fail(new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null));
            }
        });

        return promise.future();
    }
}
