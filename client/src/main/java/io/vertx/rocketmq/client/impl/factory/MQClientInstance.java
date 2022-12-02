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
package io.vertx.rocketmq.client.impl.factory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.future.CompositeFutureImpl;
import io.vertx.rocketmq.client.ReactiveUtil;
import io.vertx.rocketmq.client.common.RocketmqOptions;
import io.vertx.rocketmq.client.exception.MQClientException;
import io.vertx.rocketmq.client.impl.FindBrokerResult;
import io.vertx.rocketmq.client.impl.MQAdminImpl;
import io.vertx.rocketmq.client.impl.MQClientAPIImpl;
import io.vertx.rocketmq.client.impl.MQClientManager;
import io.vertx.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import io.vertx.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import io.vertx.rocketmq.client.impl.consumer.MQConsumerInner;
import io.vertx.rocketmq.client.impl.consumer.ProcessQueue;
import io.vertx.rocketmq.client.impl.consumer.PullMessageService;
import io.vertx.rocketmq.client.impl.consumer.RebalanceService;
import io.vertx.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import io.vertx.rocketmq.client.impl.producer.MQProducerInner;
import io.vertx.rocketmq.client.impl.producer.TopicPublishInfo;
import io.vertx.rocketmq.client.log.ClientLogger;
import io.vertx.rocketmq.client.producer.DefaultMQProducer;
import io.vertx.rocketmq.client.stat.ConsumerStatsManager;

public class MQClientInstance {
    private final static long LOCK_TIMEOUT_MILLIS = 3000;
    private final InternalLogger log = ClientLogger.getLog();
    private final String clientId;
    private final long bootTimestamp = System.currentTimeMillis();
    private final ConcurrentMap<String/* group */, MQProducerInner> producerTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<String/* group */, MQConsumerInner> consumerTable = new ConcurrentHashMap<>();
    private final MQClientAPIImpl mQClientAPIImpl;
    private final MQAdminImpl mQAdminImpl;
    private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();
    private final Lock lockNamesrv = new ReentrantLock();
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable = new ConcurrentHashMap<>();
    private final ConcurrentMap<String/* Broker Name */, HashMap<String/* address */, Integer>> brokerVersionTable = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(r -> new Thread(r, "MQClientFactoryScheduledThread"));
    private final PullMessageService pullMessageService;
    private final RebalanceService rebalanceService;
    private final DefaultMQProducer defaultMQProducer;
    private final ConsumerStatsManager consumerStatsManager;
    private final AtomicLong sendHeartbeatTimesTotal = new AtomicLong(0);
    private AtomicReference<ServiceState> serviceState = new AtomicReference<>(ServiceState.CREATE_JUST);
    private final Random random = new Random();
    private final RocketmqOptions options;

    public MQClientInstance(int instanceIndex, String clientId, VertxInternal vertx, RocketmqOptions options) {
        this.mQClientAPIImpl = new MQClientAPIImpl(vertx, options, this);
        this.options = options;
        if (this.options.getNamesrvAddr() != null) {
            this.mQClientAPIImpl.updateNameServerAddressList(this.options.getNamesrvAddr());
            log.info("user specified name server address: {}", this.options.getNamesrvAddr());
        }

        this.clientId = clientId;

        this.mQAdminImpl = new MQAdminImpl(this);

        this.pullMessageService = new PullMessageService(this, vertx);

        this.rebalanceService = new RebalanceService(this);

        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP, vertx,
                options.copyWithConditon(MixAll.CLIENT_INNER_PRODUCER_GROUP));

        this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);

        log.info("Created a new client Instance, InstanceIndex:{}, ClientID:{}, ClientVersion:{}, SerializerType:{}",
                instanceIndex,
                this.clientId,
                MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION), RemotingCommand.getSerializeTypeConfigInThisServer());
    }

    public static TopicPublishInfo topicRouteData2TopicPublishInfo(final String topic, final TopicRouteData route) {
        TopicPublishInfo info = new TopicPublishInfo();
        info.setTopicRouteData(route);
        if (route.getOrderTopicConf() != null && route.getOrderTopicConf().length() > 0) {
            String[] brokers = route.getOrderTopicConf().split(";");
            for (String broker : brokers) {
                String[] item = broker.split(":");
                int nums = Integer.parseInt(item[1]);
                for (int i = 0; i < nums; i++) {
                    MessageQueue mq = new MessageQueue(topic, item[0], i);
                    info.getMessageQueueList().add(mq);
                }
            }

            info.setOrderTopic(true);
        } else {
            List<QueueData> qds = route.getQueueDatas();
            Collections.sort(qds);
            for (QueueData qd : qds) {
                if (PermName.isWriteable(qd.getPerm())) {
                    BrokerData brokerData = null;
                    for (BrokerData bd : route.getBrokerDatas()) {
                        if (bd.getBrokerName().equals(qd.getBrokerName())) {
                            brokerData = bd;
                            break;
                        }
                    }

                    if (null == brokerData) {
                        continue;
                    }

                    if (!brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                        continue;
                    }

                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                        info.getMessageQueueList().add(mq);
                    }
                }
            }

            info.setOrderTopic(false);
        }

        return info;
    }

    public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(final String topic, final TopicRouteData route) {
        Set<MessageQueue> mqList = new HashSet<>();
        List<QueueData> qds = route.getQueueDatas();
        for (QueueData qd : qds) {
            if (PermName.isReadable(qd.getPerm())) {
                for (int i = 0; i < qd.getReadQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                    mqList.add(mq);
                }
            }
        }

        return mqList;
    }

    public void start() throws MQClientException {

        if (this.serviceState.compareAndSet(ServiceState.CREATE_JUST, ServiceState.START_FAILED)) {
            // If not specified,looking address from name server
            if (null == this.options.getNamesrvAddr()) {
                ReactiveUtil.waitOne(this.mQClientAPIImpl.fetchNameServerAddr());
            }
            // Start various schedule tasks
            this.startScheduledTask();
            // Start pull service
            this.pullMessageService.start();
            // Start rebalance service
            this.rebalanceService.start();
            // Start push service
            this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
            log.info("the client factory [{}] start OK", this.clientId);
            serviceState.set(ServiceState.RUNNING);
        } else if (this.serviceState.get() == ServiceState.START_FAILED) {
            throw new MQClientException(
                    "The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
        }
    }

    private void startScheduledTask() {
        if (null == this.options.getNamesrvAddr()) {
            this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                Future<String> future = mQClientAPIImpl.fetchNameServerAddr();
                future.onFailure(e -> log.error("ScheduledTask fetchNameServerAddr exception", e));
                ReactiveUtil.waitOne(future);
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);

        }

        this.scheduledExecutorService.scheduleAtFixedRate(() -> updateTopicRouteInfoFromNameServer(),
                10, this.options.getPollNameServerInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                MQClientInstance.this.cleanOfflineBroker();
                ReactiveUtil.waitOne(MQClientInstance.this.sendHeartbeatToAllBrokerWithLock());
            } catch (Exception e) {
                log.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
            }
        }, 1000, this.options.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                ReactiveUtil.waitOne(MQClientInstance.this.persistAllConsumerOffset());
            } catch (Exception e) {
                log.error("ScheduledTask persistAllConsumerOffset exception", e);
            }
        }, 1000 * 10, this.options.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                MQClientInstance.this.adjustThreadPool();
            } catch (Exception e) {
                log.error("ScheduledTask adjustThreadPool exception", e);
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public String getClientId() {
        return clientId;
    }

    public void updateTopicRouteInfoFromNameServer() {
        Set<String> topicList = new HashSet<>();

        // Consumer
        {
            for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    Set<SubscriptionData> subList = impl.subscriptions();
                    if (subList != null) {
                        for (SubscriptionData subData : subList) {
                            topicList.add(subData.getTopic());
                        }
                    }
                }
            }
        }

        // Producer
        {
            for (Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    Set<String> lst = impl.getPublishTopicList();
                    topicList.addAll(lst);
                }
            }
        }

        for (String topic : topicList) {
            ReactiveUtil.waitOne(this.updateTopicRouteInfoFromNameServer(topic));
        }
    }

    /**
     * @return newOffsetTable
     */
    public Map<MessageQueue, Long> parseOffsetTableFromBroker(Map<MessageQueue, Long> offsetTable, String namespace) {
        HashMap<MessageQueue, Long> newOffsetTable = new HashMap<>();
        if (StringUtils.isNotEmpty(namespace)) {
            for (Entry<MessageQueue, Long> entry : offsetTable.entrySet()) {
                MessageQueue queue = entry.getKey();
                queue.setTopic(NamespaceUtil.withoutNamespace(queue.getTopic(), namespace));
                newOffsetTable.put(queue, entry.getValue());
            }
        } else {
            newOffsetTable.putAll(offsetTable);
        }

        return newOffsetTable;
    }

    /**
     * Remove offline broker
     */
    private void cleanOfflineBroker() {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                try {
                    ConcurrentHashMap<String, HashMap<Long, String>> updatedTable = new ConcurrentHashMap<>();
                    Iterator<Entry<String, HashMap<Long, String>>> itBrokerTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerTable.hasNext()) {
                        Entry<String, HashMap<Long, String>> entry = itBrokerTable.next();
                        String brokerName = entry.getKey();
                        HashMap<Long, String> oneTable = entry.getValue();
                        HashMap<Long, String> cloneAddrTable = new HashMap<>(oneTable);
                        Iterator<Entry<Long, String>> it = cloneAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> ee = it.next();
                            String addr = ee.getValue();
                            if (!this.isBrokerAddrExistInTopicRouteTable(addr)) {
                                it.remove();
                                log.info("the broker addr[{} {}] is offline, remove it", brokerName, addr);
                            }
                        }

                        if (cloneAddrTable.isEmpty()) {
                            itBrokerTable.remove();
                            log.info("the broker[{}] name's host is offline, remove it", brokerName);
                        } else {
                            updatedTable.put(brokerName, cloneAddrTable);
                        }
                    }

                    if (!updatedTable.isEmpty()) {
                        this.brokerAddrTable.putAll(updatedTable);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
        } catch (InterruptedException e) {
            log.warn("cleanOfflineBroker Exception", e);
        }
    }

    public Future<Void> checkClientInBroker() {

        List<Future<Void>> rs = new ArrayList<>();
        Promise<Void> promise = Promise.promise();
        for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            Set<SubscriptionData> subscriptionInner = entry.getValue().subscriptions();
            if (subscriptionInner == null || subscriptionInner.isEmpty()) {
                promise.complete();
                return promise.future();
            }

            for (SubscriptionData subscriptionData : subscriptionInner) {
                if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    continue;
                }
                // may need to check one broker every cluster...
                // assume that the configs of every broker in cluster are the the same.
                String addr = findBrokerAddrByTopic(subscriptionData.getTopic());

                if (addr != null) {
                    Future<Void> voidFuture = this.getMQClientAPIImpl().checkClientInBroker(
                            addr, entry.getKey(), this.clientId, subscriptionData, 3 * 1000);
                    voidFuture.onComplete(ar -> {
                        if (ar.failed()) {
                            log.error("Check client in broker error, maybe because you use "
                                    + subscriptionData.getExpressionType()
                                    + " to filter message, but server has not been upgraded to support!"
                                    + "This error would not affect the launch of consumer, but may has impact on message receiving if you "
                                    +
                                    "have use the new features which are not supported by server, please check the log!",
                                    ar.cause());
                        }
                    });
                    rs.add(voidFuture);
                }
            }
        }

        CompositeFutureImpl.all(rs.toArray(new Future[0])).onFailure(promise::fail).onSuccess(vs -> promise.complete());
        return promise.future();
    }

    public Future<Void> sendHeartbeatToAllBrokerWithLock() {
        return this.sendHeartbeatToAllBroker().compose(v -> this.uploadFilterClassSource());
    }

    private Future<Void> persistAllConsumerOffset() {
        List<Future<Void>> rs = new ArrayList<>();
        for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            rs.add(impl.persistConsumerOffset());
        }

        Promise<Void> promise = Promise.promise();
        CompositeFutureImpl.all(rs.toArray(new Future[0])).onFailure(promise::fail).onSuccess(v -> promise.complete());
        return promise.future();
    }

    public void adjustThreadPool() {
        for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    if (impl instanceof DefaultMQPushConsumerImpl) {
                        DefaultMQPushConsumerImpl dmq = (DefaultMQPushConsumerImpl) impl;
                        dmq.adjustThreadPool();
                    }
                } catch (Exception ignored) {
                }
            }
        }
    }

    public Future<Boolean> updateTopicRouteInfoFromNameServer(final String topic) {
        return updateTopicRouteInfoFromNameServer(topic, false, null);
    }

    private boolean isBrokerAddrExistInTopicRouteTable(final String addr) {
        for (Entry<String, TopicRouteData> entry : this.topicRouteTable.entrySet()) {
            TopicRouteData topicRouteData = entry.getValue();
            List<BrokerData> bds = topicRouteData.getBrokerDatas();
            for (BrokerData bd : bds) {
                if (bd.getBrokerAddrs() != null) {
                    boolean exist = bd.getBrokerAddrs().containsValue(addr);
                    if (exist)
                        return true;
                }
            }
        }

        return false;
    }

    private Future<Void> sendHeartbeatToAllBroker() {
        final HeartbeatData heartbeatData = this.prepareHeartbeatData();
        final boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
        final boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
        if (producerEmpty && consumerEmpty) {
            log.warn("sending heartbeat, but no consumer and no producer");
            return Future.succeededFuture();
        }
        if (this.brokerAddrTable.isEmpty()) {
            return Future.succeededFuture();
        }

        Promise<Void> promise = Promise.promise();
        long times = this.sendHeartbeatTimesTotal.getAndIncrement();
        List<Future<Integer>> rs = new ArrayList<>();
        for (Entry<String, HashMap<Long, String>> entry : this.brokerAddrTable.entrySet()) {
            String brokerName = entry.getKey();
            HashMap<Long, String> oneTable = entry.getValue();
            if (oneTable == null) {
                continue;
            }

            for (Entry<Long, String> entry1 : oneTable.entrySet()) {
                Long id = entry1.getKey();
                String addr = entry1.getValue();
                if (addr == null) {
                    continue;
                }
                if (consumerEmpty) {
                    if (id != MixAll.MASTER_ID)
                        continue;
                }
                HashMap<String, Integer> preHashMap = this.brokerVersionTable.computeIfAbsent(brokerName,
                        k -> new HashMap<>(4));
                Future<Integer> future = this.mQClientAPIImpl.sendHearbeat(addr, heartbeatData, 3000);
                future.onFailure(e -> {
                    if (this.isBrokerInNameServer(addr)) {
                        log.info("send heart beat to broker[{} {} {}] failed", brokerName, id, addr, e);
                    } else {
                        log.info("send heart beat to broker[{} {} {}] exception, because the broker not up, forget it",
                                brokerName,
                                id, addr, e);
                    }
                });
                future.onSuccess(version -> {
                    preHashMap.put(addr, version);
                    if (times % 20 == 0) {
                        log.info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
                        log.info(heartbeatData.toString());
                    }
                });
                rs.add(future);
            }
        }

        CompositeFutureImpl.any(rs.toArray(new Future[0])).onFailure(promise::fail).onSuccess(cf -> promise.complete());
        return promise.future();
    }

    private Future<Void> uploadFilterClassSource() {
        List<Future<Void>> rs = new ArrayList<>();
        for (Entry<String, MQConsumerInner> next : this.consumerTable.entrySet()) {
            MQConsumerInner consumer = next.getValue();
            if (ConsumeType.CONSUME_PASSIVELY == consumer.consumeType()) {
                Set<SubscriptionData> subscriptions = consumer.subscriptions();
                for (SubscriptionData sub : subscriptions) {
                    if (sub.isClassFilterMode() && sub.getFilterClassSource() != null) {
                        final String consumerGroup = consumer.groupName();
                        final String className = sub.getSubString();
                        final String topic = sub.getTopic();
                        final String filterClassSource = sub.getFilterClassSource();
                        rs.add(this.uploadFilterClassToAllFilterServer(consumerGroup, className, topic, filterClassSource));
                    }
                }
            }
        }
        Promise<Void> promise = Promise.promise();
        CompositeFutureImpl.any(rs.toArray(new Future[0])).onFailure(promise::fail).onSuccess(cf -> promise.complete());
        return promise.future();
    }

    public Future<Boolean> updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault,
            DefaultMQProducer defaultMQProducer) {
        Promise<Boolean> result = Promise.promise();

        Promise<TopicRouteData> promise = Promise.promise();
        Future<TopicRouteData> topicRouteInfoFutrue;
        if (isDefault && defaultMQProducer != null) {
            topicRouteInfoFutrue = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(
                    defaultMQProducer.getCreateTopicKey(),
                    1000 * 3);
            topicRouteInfoFutrue.onFailure(promise::fail);
            topicRouteInfoFutrue.onSuccess(topicRouteData -> {
                if (topicRouteData != null) {
                    for (QueueData data : topicRouteData.getQueueDatas()) {
                        int queueNums = Math.min(defaultMQProducer.getDefaultTopicQueueNums(), data.getReadQueueNums());
                        data.setReadQueueNums(queueNums);
                        data.setWriteQueueNums(queueNums);
                    }
                }
                promise.complete(topicRouteData);
            });

        } else {
            topicRouteInfoFutrue = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, 1000 * 3);
            topicRouteInfoFutrue.onFailure(promise::fail);
            topicRouteInfoFutrue.onSuccess(promise::complete);
        }
        promise.future().onFailure(e -> {
            //                    this.lockNamesrv.unlock();
            if (e instanceof MQClientException) {
                if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("updateTopicRouteInfoFromNameServer Exception", e);
                }
                result.complete(Boolean.FALSE);
            } else if (e instanceof RemotingException) {
                log.error("updateTopicRouteInfoFromNameServer Exception", e);
                result.fail(new IllegalStateException(e));
            } else {
                result.fail(e);
            }
        });
        promise.future().onSuccess(topicRouteData -> {
            if (topicRouteData != null) {
                ContextInternal.current().executeBlocking(blockPromise -> {
                    TopicRouteData old = this.topicRouteTable.get(topic);
                    boolean changed = topicRouteDataIsChange(old, topicRouteData);
                    if (!changed) {
                        changed = this.isNeedUpdateTopicRouteInfo(topic);
                    } else {
                        log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                    }

                    if (changed) {
                        TopicRouteData cloneTopicRouteData = topicRouteData.cloneTopicRouteData();

                        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                            this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                        }

                        // Update Pub info
                        {
                            TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                            publishInfo.setHaveTopicRouterInfo(true);
                            for (Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
                                MQProducerInner impl = entry.getValue();
                                if (impl != null) {
                                    impl.updateTopicPublishInfo(topic, publishInfo);
                                }
                            }
                        }

                        // Update sub info
                        {
                            Set<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic,
                                    topicRouteData);
                            for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
                                MQConsumerInner impl = entry.getValue();
                                if (impl != null) {
                                    impl.updateTopicSubscribeInfo(topic, subscribeInfo);
                                }
                            }
                        }
                        log.info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);
                        this.topicRouteTable.put(topic, cloneTopicRouteData);
                        blockPromise.complete(Boolean.TRUE);
                    } else {
                        blockPromise.complete(Boolean.FALSE);
                    }
                }).onFailure(result::fail).onSuccess(r -> result.complete((boolean) r));
            } else {
                log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}",
                        topic);
                result.complete(Boolean.FALSE);
            }
        });

        return result.future();
    }

    private HeartbeatData prepareHeartbeatData() {
        HeartbeatData heartbeatData = new HeartbeatData();

        // clientID
        heartbeatData.setClientID(this.clientId);

        // Consumer
        for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                ConsumerData consumerData = new ConsumerData();
                consumerData.setGroupName(impl.groupName());
                consumerData.setConsumeType(impl.consumeType());
                consumerData.setMessageModel(impl.messageModel());
                consumerData.setConsumeFromWhere(impl.consumeFromWhere());
                consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
                consumerData.setUnitMode(impl.isUnitMode());

                heartbeatData.getConsumerDataSet().add(consumerData);
            }
        }

        // Producer
        for (Entry<String/* group */, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner impl = entry.getValue();
            if (impl != null) {
                ProducerData producerData = new ProducerData();
                producerData.setGroupName(entry.getKey());

                heartbeatData.getProducerDataSet().add(producerData);
            }
        }

        return heartbeatData;
    }

    private boolean isBrokerInNameServer(final String brokerAddr) {
        for (Entry<String, TopicRouteData> itNext : this.topicRouteTable.entrySet()) {
            List<BrokerData> brokerDatas = itNext.getValue().getBrokerDatas();
            for (BrokerData bd : brokerDatas) {
                boolean contain = bd.getBrokerAddrs().containsValue(brokerAddr);
                if (contain)
                    return true;
            }
        }

        return false;
    }

    /**
     * This method will be removed in the version 5.0.0,because filterServer was removed,and method
     * <code>subscribe(final String topic, final MessageSelector messageSelector)</code> is recommended.
     */
    @Deprecated
    private Future<Void> uploadFilterClassToAllFilterServer(final String consumerGroup, final String fullClassName,
            final String topic,
            final String filterClassSource) {

        byte[] classBody = null;
        int classCRC = 0;
        try {
            classBody = filterClassSource.getBytes(MixAll.DEFAULT_CHARSET);
            classCRC = UtilAll.crc32(classBody);
        } catch (Exception e1) {
            log.warn("uploadFilterClassToAllFilterServer Exception, ClassName: {} {}",
                    fullClassName,
                    RemotingHelper.exceptionSimpleDesc(e1));
        }

        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        Promise<Void> promise = Promise.promise();
        if (topicRouteData != null
                && topicRouteData.getFilterServerTable() != null && !topicRouteData.getFilterServerTable().isEmpty()) {
            List<Future<Void>> rs = new ArrayList<>();
            for (Entry<String, List<String>> next : topicRouteData.getFilterServerTable().entrySet()) {
                List<String> value = next.getValue();
                for (final String fsAddr : value) {
                    Future<Void> future = this.mQClientAPIImpl.registerMessageFilterClass(fsAddr, consumerGroup, topic,
                            fullClassName, classCRC, classBody,
                            5000);
                    future.onFailure(e -> log.error("uploadFilterClassToAllFilterServer Exception", e));
                    future.onSuccess(
                            v -> log.info("register message class filter to {} OK, ConsumerGroup: {} Topic: {} ClassName: {}",
                                    fsAddr, consumerGroup,
                                    topic, fullClassName));
                    rs.add(future);
                }
            }
            CompositeFutureImpl.any(rs.toArray(new Future[0])).onFailure(promise::fail).onSuccess(cf -> promise.complete());
        } else {
            log.warn(
                    "register message class filter failed, because no filter server, ConsumerGroup: {} Topic: {} ClassName: {}",
                    consumerGroup, topic, fullClassName);
            promise.complete();
        }

        return promise.future();
    }

    private boolean topicRouteDataIsChange(TopicRouteData olddata, TopicRouteData nowdata) {
        if (olddata == null || nowdata == null)
            return true;
        TopicRouteData old = olddata.cloneTopicRouteData();
        TopicRouteData now = nowdata.cloneTopicRouteData();
        Collections.sort(old.getQueueDatas());
        Collections.sort(old.getBrokerDatas());
        Collections.sort(now.getQueueDatas());
        Collections.sort(now.getBrokerDatas());
        return !old.equals(now);

    }

    private boolean isNeedUpdateTopicRouteInfo(final String topic) {
        boolean result = false;
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isPublishTopicNeedUpdate(topic);
                }
            }
        }

        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isSubscribeTopicNeedUpdate(topic);
                }
            }
        }

        return result;
    }

    public Future<Void> shutdown() {
        // Consumer
        if (!this.consumerTable.isEmpty())
            return Future.succeededFuture();

        // Producer
        if (this.producerTable.size() > 1)
            return Future.succeededFuture();

        if (this.serviceState.compareAndSet(ServiceState.RUNNING, ServiceState.SHUTDOWN_ALREADY)) {
            this.defaultMQProducer.getDefaultMQProducerImpl().shutdown(false);
            this.pullMessageService.shutdown(true);
            this.scheduledExecutorService.shutdown();
            this.rebalanceService.shutdown();

            MQClientManager.getInstance().removeClientFactory(this.clientId);
            log.info("the client factory [{}] shutdown OK", this.clientId);
            return this.mQClientAPIImpl.shutdown();
        }

        return Future.succeededFuture();
    }

    public boolean registerConsumer(final String group, final MQConsumerInner consumer) {
        if (null == group || null == consumer) {
            return false;
        }

        MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
        if (prev != null) {
            log.warn("the consumer group[" + group + "] exist already.");
            return false;
        }

        return true;
    }

    public Future<Void> unregisterConsumer(final String group) {
        this.consumerTable.remove(group);
        return this.unregisterClient(null, group);
    }

    private Future<Void> unregisterClient(final String producerGroup, final String consumerGroup) {
        List<Future<Void>> rs = new ArrayList<>();
        for (Entry<String, HashMap<Long, String>> entry : this.brokerAddrTable.entrySet()) {
            String brokerName = entry.getKey();
            HashMap<Long, String> oneTable = entry.getValue();

            if (oneTable != null) {
                for (Entry<Long, String> entry1 : oneTable.entrySet()) {
                    String addr = entry1.getValue();
                    if (addr != null) {
                        Future<Void> voidFuture = this.mQClientAPIImpl.unregisterClient(addr, this.clientId, producerGroup,
                                consumerGroup, 3000);
                        voidFuture.onFailure(e -> log.error("unregister client exception from broker: " + addr, e));
                        voidFuture.onSuccess(
                                v -> log.info("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success",
                                        producerGroup, consumerGroup, brokerName, entry1.getKey(), addr));
                        rs.add(voidFuture);
                    }
                }
            }
        }
        Promise<Void> promise = Promise.promise();
        CompositeFutureImpl.any(rs.toArray(new Future[0])).onComplete(ar -> promise.complete());
        return promise.future();
    }

    public boolean registerProducer(final String group, final DefaultMQProducerImpl producer) {
        if (null == group || null == producer) {
            return false;
        }

        MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
        if (prev != null) {
            log.warn("the producer group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public Future<Void> unregisterProducer(final String group) {
        this.producerTable.remove(group);
        return this.unregisterClient(group, null);
    }

    public void rebalanceImmediately() {
        this.rebalanceService.wakeup();
    }

    public void doRebalance() {
        for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    impl.doRebalance();
                } catch (Throwable e) {
                    log.error("doRebalance exception", e);
                }
            }
        }
    }

    public MQProducerInner selectProducer(final String group) {
        return this.producerTable.get(group);
    }

    public MQConsumerInner selectConsumer(final String group) {
        return this.consumerTable.get(group);
    }

    public FindBrokerResult findBrokerAddressInAdmin(final String brokerName) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            for (Entry<Long, String> entry : map.entrySet()) {
                Long id = entry.getKey();
                brokerAddr = entry.getValue();
                if (brokerAddr != null) {
                    found = true;
                    slave = MixAll.MASTER_ID != id;
                    break;

                }
            } // end of for
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    public String findBrokerAddressInPublish(final String brokerName) {
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(MixAll.MASTER_ID);
        }

        return null;
    }

    public FindBrokerResult findBrokerAddressInSubscribe(
            final String brokerName,
            final long brokerId,
            final boolean onlyThisBroker) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            brokerAddr = map.get(brokerId);
            slave = brokerId != MixAll.MASTER_ID;
            found = brokerAddr != null;

            if (!found && !onlyThisBroker) {
                Entry<Long, String> entry = map.entrySet().iterator().next();
                brokerAddr = entry.getValue();
                slave = entry.getKey() != MixAll.MASTER_ID;
                found = true;
            }
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    public int findBrokerVersion(String brokerName, String brokerAddr) {
        if (this.brokerVersionTable.containsKey(brokerName)) {
            if (this.brokerVersionTable.get(brokerName).containsKey(brokerAddr)) {
                return this.brokerVersionTable.get(brokerName).get(brokerAddr);
            }
        }
        //To do need to fresh the version
        return 0;
    }

    public Future<List<String>> findConsumerIdList(final String topic, final String group) {
        Promise<List<String>> promise = Promise.promise();
        Promise<String> brokerAddrPromise = Promise.promise();
        String brokerAddr = this.findBrokerAddrByTopic(topic);
        if (null == brokerAddr) {
            Future<Boolean> booleanFuture = this.updateTopicRouteInfoFromNameServer(topic);
            booleanFuture.onFailure(brokerAddrPromise::fail);
            booleanFuture.onSuccess(bool -> brokerAddrPromise.complete(this.findBrokerAddrByTopic(topic)));
        } else {
            brokerAddrPromise.complete(brokerAddr);
        }

        brokerAddrPromise.future().onFailure(promise::fail);
        brokerAddrPromise.future().onSuccess(otherBrokerAddr -> {
            if (null != otherBrokerAddr) {
                Future<List<String>> consumerIdListByGroup = this.mQClientAPIImpl.getConsumerIdListByGroup(otherBrokerAddr,
                        group,
                        3000);
                consumerIdListByGroup.onComplete(ar -> {
                    if (ar.failed()) {
                        log.warn("getConsumerIdListByGroup exception, " + otherBrokerAddr + " " + group, ar.cause());
                        promise.complete();
                    } else {
                        promise.complete(ar.result());
                    }
                });
            } else {
                promise.complete();
            }
        });

        return promise.future();
    }

    public String findBrokerAddrByTopic(final String topic) {
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null) {
            List<BrokerData> brokers = topicRouteData.getBrokerDatas();
            if (!brokers.isEmpty()) {
                int index = random.nextInt(brokers.size());
                BrokerData bd = brokers.get(index % brokers.size());
                return bd.selectBrokerAddr();
            }
        }

        return null;
    }

    public Future<Void> resetOffset(String topic, String group, Map<MessageQueue, Long> offsetTable) {
        DefaultMQPushConsumerImpl consumer;
        MQConsumerInner impl = this.consumerTable.get(group);
        Promise<Void> promise = Promise.promise();
        if (impl instanceof DefaultMQPushConsumerImpl) {
            consumer = (DefaultMQPushConsumerImpl) impl;
        } else {
            log.info("[reset-offset] consumer dose not exist. group={}", group);
            promise.complete();
            return promise.future();
        }
        consumer.suspend();

        ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();
        for (Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (topic.equals(mq.getTopic()) && offsetTable.containsKey(mq)) {
                ProcessQueue pq = entry.getValue();
                pq.setDropped(true);
                pq.clear();
            }
        }

        List<Future<Boolean>> results = new ArrayList<>();
        Iterator<MessageQueue> iterator = processQueueTable.keySet().iterator();
        while (iterator.hasNext()) {
            MessageQueue mq = iterator.next();
            Long offset = offsetTable.get(mq);
            if (topic.equals(mq.getTopic()) && offset != null) {
                consumer.updateConsumeOffset(mq, offset);
                Future<Boolean> booleanFuture = consumer.getRebalanceImpl().removeUnnecessaryMessageQueue(mq,
                        processQueueTable.get(mq));
                booleanFuture.onFailure(e -> log.warn("reset offset failed. group={}, {}", group, mq, e));
                booleanFuture.onSuccess(bool -> iterator.remove());
                results.add(booleanFuture);
            }
        }

        DefaultMQPushConsumerImpl finalConsumer = consumer;
        CompositeFutureImpl.any(results.toArray(new Future[0])).onComplete(ar -> {
            finalConsumer.resume();
            promise.complete();
        });

        return promise.future();
    }

    public Map<MessageQueue, Long> getConsumerStatus(String topic, String group) {
        MQConsumerInner impl = this.consumerTable.get(group);
        if (impl instanceof DefaultMQPushConsumerImpl) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else if (impl instanceof DefaultMQPullConsumerImpl) {
            DefaultMQPullConsumerImpl consumer = (DefaultMQPullConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else {
            return Collections.EMPTY_MAP;
        }
    }

    public TopicRouteData getAnExistTopicRouteData(final String topic) {
        return this.topicRouteTable.get(topic);
    }

    public MQClientAPIImpl getMQClientAPIImpl() {
        return mQClientAPIImpl;
    }

    public MQAdminImpl getMQAdminImpl() {
        return mQAdminImpl;
    }

    public long getBootTimestamp() {
        return bootTimestamp;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public PullMessageService getPullMessageService() {
        return pullMessageService;
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }

    public ConcurrentMap<String, TopicRouteData> getTopicRouteTable() {
        return topicRouteTable;
    }

    public Future<ConsumeMessageDirectlyResult> consumeMessageDirectly(final MessageExt msg,
            final String consumerGroup,
            final String brokerName) {
        Promise<ConsumeMessageDirectlyResult> promise = Promise.promise();
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        if (null != mqConsumerInner) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) mqConsumerInner;
            consumer.getConsumeMessageService().consumeMessageDirectly(msg, brokerName)
                    .onFailure(promise::fail).onSuccess(promise::complete);
        } else {
            promise.complete();
        }

        return promise.future();
    }

    public Future<ConsumerRunningInfo> consumerRunningInfo(final String consumerGroup) {
        Promise<ConsumerRunningInfo> promise = Promise.promise();
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        Future<ConsumerRunningInfo> consumerRunningInfoFuture = mqConsumerInner.consumerRunningInfo();
        consumerRunningInfoFuture.onFailure(promise::fail);
        consumerRunningInfoFuture.onSuccess(consumerRunningInfo -> {
            List<String> nsList = this.mQClientAPIImpl.getNameServerAddressList();
            StringBuilder strBuilder = new StringBuilder();
            if (nsList != null) {
                for (String addr : nsList) {
                    strBuilder.append(addr).append(";");
                }
            }

            String nsAddr = strBuilder.toString();
            consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, nsAddr);
            consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CONSUME_TYPE,
                    mqConsumerInner.consumeType().name());
            consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CLIENT_VERSION,
                    MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));
        });

        return promise.future();
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return consumerStatsManager;
    }

    public RocketmqOptions getClientConfig() {
        return options;
    }
}
