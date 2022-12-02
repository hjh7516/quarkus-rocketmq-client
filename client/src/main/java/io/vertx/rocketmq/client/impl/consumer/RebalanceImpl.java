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

import static org.apache.rocketmq.common.protocol.heartbeat.ConsumeType.CONSUME_PASSIVELY;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.InternalLogger;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.future.CompositeFutureImpl;
import io.vertx.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import io.vertx.rocketmq.client.impl.FindBrokerResult;
import io.vertx.rocketmq.client.impl.factory.MQClientInstance;
import io.vertx.rocketmq.client.log.ClientLogger;

public abstract class RebalanceImpl {
    protected static final InternalLogger log = ClientLogger.getLog();
    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<>(64);
    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable = new ConcurrentHashMap<>();
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner = new ConcurrentHashMap<>();
    protected String consumerGroup;
    protected MessageModel messageModel;
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    protected MQClientInstance mQClientFactory;

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
            AllocateMessageQueueStrategy allocateMessageQueueStrategy,
            MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    public Future<Void> unlock(final MessageQueue mq, final boolean oneway) {
        Promise<Void> promise = Promise.promise();
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            Future<Void> voidFuture = this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(),
                    requestBody, 1000, oneway);
            voidFuture.onComplete(ar -> {
                if (ar.failed()) {
                    log.error("unlockBatchMQ exception, " + mq, ar.cause());
                } else {
                    log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                            this.consumerGroup,
                            this.mQClientFactory.getClientId(),
                            mq);
                }
                promise.complete();
            });
        } else {
            promise.complete();
        }

        return promise.future();
    }

    public Future<Void> unlockAll(final boolean oneway) {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        Promise<Void> promise = Promise.promise();
        List<Future> resultFuturs = new ArrayList<>();
        for (final Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID,
                    true);
            if (findBrokerResult != null) {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                Future<Void> voidFuture = this.mQClientFactory.getMQClientAPIImpl()
                        .unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                voidFuture.onFailure(e -> log.error("unlockBatchMQ exception, " + mqs, e));
                voidFuture.onSuccess(v -> {
                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                });

                resultFuturs.add(voidFuture);
            }
        }

        CompositeFuture.any(resultFuturs).onComplete(ar -> promise.complete());
        return promise.future();
    }

    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String, Set<MessageQueue>> result = new HashMap<>();
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            Set<MessageQueue> mqs = result.computeIfAbsent(mq.getBrokerName(), k -> new HashSet<>());
            mqs.add(mq);
        }
        return result;
    }

    public Future<Boolean> lock(final MessageQueue mq) {
        Promise<Boolean> promise = Promise.promise();
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            Future<Set<MessageQueue>> setFuture = this.mQClientFactory.getMQClientAPIImpl()
                    .lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
            setFuture.onComplete(ar -> {
                boolean result = false;
                if (ar.failed()) {
                    log.error("lockBatchMQ exception, " + mq, ar.cause());
                } else {
                    Set<MessageQueue> lockedMq = ar.result();
                    for (MessageQueue mmqq : lockedMq) {
                        ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                        if (processQueue != null) {
                            processQueue.setLocked(true);
                            processQueue.setLastLockTimestamp(System.currentTimeMillis());
                        }
                    }

                    boolean lockOK = lockedMq.contains(mq);
                    log.info("the message queue lock {}, {} {}",
                            lockOK ? "OK" : "Failed",
                            this.consumerGroup,
                            mq);
                    result = lockOK;
                }
                promise.complete(result);
            });
        } else {
            promise.complete(false);
        }

        return promise.future();
    }

    public Future<Void> lockAll() {
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();
        Promise<Void> promise = Promise.promise();
        List<Future> resultFutures = new ArrayList<>();
        for (Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty())
                continue;

            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID,
                    true);
            if (findBrokerResult != null) {
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                Future<Set<MessageQueue>> setFuture = this.mQClientFactory.getMQClientAPIImpl()
                        .lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                setFuture.onFailure(e -> log.error("lockBatchMQ exception, " + mqs, e));
                setFuture.onSuccess(lockOKMQSet -> {
                    for (MessageQueue mq : lockOKMQSet) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            if (!processQueue.isLocked()) {
                                log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                            }

                            processQueue.setLocked(true);
                            processQueue.setLastLockTimestamp(System.currentTimeMillis());
                        }
                    }
                    for (MessageQueue mq : mqs) {
                        if (!lockOKMQSet.contains(mq)) {
                            ProcessQueue processQueue = this.processQueueTable.get(mq);
                            if (processQueue != null) {
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                });
                resultFutures.add(setFuture);
            }
        }

        CompositeFuture.any(resultFutures).onComplete(ar -> promise.complete());
        return promise.future();
    }

    public Future<Void> doRebalance(final boolean isOrder) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        List<Future<Void>> results = new ArrayList<>();
        if (subTable != null) {
            for (final Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                results.add(this.rebalanceByTopic(topic, isOrder).onFailure(e -> {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalanceByTopic Exception", e);
                    }
                }));
            }
        }

        Promise<Void> promise = Promise.promise();
        CompositeFutureImpl.any(results.toArray(new Future[0])).onComplete(ar -> {
            this.truncateMessageQueueNotMyTopic();
            promise.complete();
        });
        return promise.future();
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    private Future<Void> rebalanceByTopic(final String topic, final boolean isOrder) {
        Promise<Void> promise = Promise.promise();
        switch (messageModel) {
            case BROADCASTING: {
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                if (mqSet != null) {
                    Future<Boolean> changedFuture = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                    changedFuture.onFailure(promise::fail);
                    changedFuture.onSuccess(changed -> {
                        if (changed) {
                            this.messageQueueChanged(topic, mqSet, mqSet);
                            log.info("messageQueueChanged {} {} {} {}",
                                    consumerGroup,
                                    topic,
                                    mqSet,
                                    mqSet);
                        }
                        promise.complete();
                    });
                } else {
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }
                break;
            }
            case CLUSTERING: {
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                if (null == mqSet) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }
                Future<List<String>> cidAllFuture = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
                cidAllFuture.onFailure(promise::fail);
                cidAllFuture.onSuccess(cidAll -> {
                    if (null == cidAll) {
                        log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                    }
                    if (mqSet != null && cidAll != null) {
                        List<MessageQueue> mqAll = new ArrayList<>(mqSet);
                        Collections.sort(mqAll);
                        Collections.sort(cidAll);
                        AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;
                        List<MessageQueue> allocateResult;
                        try {
                            allocateResult = strategy.allocate(
                                    this.consumerGroup,
                                    this.mQClientFactory.getClientId(),
                                    mqAll,
                                    cidAll);
                        } catch (Throwable e) {
                            log.error("AllocateMessageQueueStrategy.allocate Exception. allocateMessageQueueStrategyName={}",
                                    strategy.getName(),
                                    e);
                            promise.complete();
                            return;
                        }

                        Set<MessageQueue> allocateResultSet = new HashSet<>();
                        if (allocateResult != null) {
                            allocateResultSet.addAll(allocateResult);
                        }

                        Future<Boolean> changedFuture = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                        changedFuture.onFailure(promise::fail);
                        changedFuture.onSuccess(changed -> {
                            if (changed) {
                                log.info(
                                        "rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                                        strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(),
                                        mqSet.size(), cidAll.size(),
                                        allocateResultSet.size(), allocateResultSet);
                                this.messageQueueChanged(topic, mqSet, allocateResultSet);
                            }
                            promise.complete();
                        });
                    }
                });
                break;
            }
            default:
                promise.complete();
                break;
        }

        return promise.future();
    }

    private void truncateMessageQueueNotMyTopic() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();

        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }
    }

    private Future<Boolean> updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet,
            final boolean isOrder) {

        List<Future<Boolean>> results = new ArrayList<>();
        final AtomicBoolean changed = new AtomicBoolean(false);
        processQueueTable.forEach((mq, pq) -> {
            if (mq.getTopic().equals(topic)) {
                Future<Boolean> booleanFuture = null;
                if (!mqSet.contains(mq)) {
                    pq.setDropped(true);
                    booleanFuture = this.removeUnnecessaryMessageQueue(mq, pq);
                    booleanFuture.onSuccess(bool -> {
                        if (bool) {
                            processQueueTable.remove(mq);
                            changed.set(true);
                            log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                        }
                    });
                } else if (pq.isPullExpired() && CONSUME_PASSIVELY.equals(this.consumeType())) {
                    booleanFuture = this.removeUnnecessaryMessageQueue(mq, pq);
                    booleanFuture.onSuccess(bool -> {
                        if (bool) {
                            processQueueTable.remove(mq);
                            changed.set(true);
                            log.error(
                                    "[BUG]doRebalance, {}, remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                                    consumerGroup, mq);
                        }
                    });
                }
                if (null != booleanFuture) {
                    results.add(booleanFuture);
                }
            }
        });

        List<Future<Boolean>> dispatchResults = new ArrayList<>();
        CompositeFutureImpl.all(results.toArray(new Future[0])).onSuccess(cf -> {
            for (MessageQueue mq : mqSet) {
                if (!this.processQueueTable.containsKey(mq)) {
                    if (isOrder) {
                        Future<Boolean> lockFuture = this.lock(mq);
                        lockFuture.onSuccess(lock -> {
                            if (lock) {
                                log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                                Future<Boolean> dispatchResult = dispatchPullRequestIfNecessary(mq).onSuccess(bool -> {
                                    if (bool) {
                                        changed.set(true);
                                    }
                                });
                                dispatchResults.add(dispatchResult);
                            }
                        });
                    } else {
                        Future<Boolean> dispatchResult = dispatchPullRequestIfNecessary(mq).onSuccess(bool -> {
                            if (bool) {
                                changed.set(true);
                            }
                        });
                        dispatchResults.add(dispatchResult);
                    }
                }
            }
        });
        Promise<Boolean> promise = Promise.promise();
        CompositeFutureImpl.all(dispatchResults.toArray(new Future[0])).onComplete(ar -> {
            if (ar.failed()) {
                promise.fail(ar.cause());
            } else {
                promise.complete(changed.get());
            }
        });

        return promise.future();
    }

    private Future<Boolean> dispatchPullRequestIfNecessary(MessageQueue mq) {
        Promise<Boolean> promise = Promise.promise();
        this.removeDirtyOffset(mq);
        ProcessQueue pq = new ProcessQueue();
        Future<Long> nextOffsetFuture = this.computePullFromWhere(mq);
        nextOffsetFuture.onFailure(promise::fail);
        nextOffsetFuture.onSuccess(nextOffset -> {
            if (nextOffset >= 0) {
                ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                if (pre != null) {
                    log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                } else {
                    log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                    PullRequest pullRequest = new PullRequest();
                    pullRequest.setConsumerGroup(consumerGroup);
                    pullRequest.setNextOffset(nextOffset);
                    pullRequest.setMessageQueue(mq);
                    pullRequest.setProcessQueue(pq);
                    dispatchPullRequest(pullRequest)
                            .onFailure(promise::fail)
                            .onSuccess(v -> promise.complete(Boolean.TRUE));
                }
            } else {
                promise.complete(Boolean.FALSE);
                log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
            }
        });
        return promise.future();
    }

    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
            final Set<MessageQueue> mqDivided);

    public abstract Future<Boolean> removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);

    public abstract Future<Long> computePullFromWhere(final MessageQueue mq);

    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList);

    public Future<Void> dispatchPullRequest(PullRequest pullRequest) {
        Promise<Void> promise = Promise.promise();
        promise.complete();
        return promise.future();
    }

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void destroy() {
        for (Entry<MessageQueue, ProcessQueue> next : this.processQueueTable.entrySet()) {
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();
    }
}
