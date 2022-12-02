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
package io.vertx.rocketmq.client.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingUtil;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.future.CompositeFutureImpl;
import io.vertx.rocketmq.client.QueryResult;
import io.vertx.rocketmq.client.Validators;
import io.vertx.rocketmq.client.exception.MQClientException;
import io.vertx.rocketmq.client.impl.factory.MQClientInstance;
import io.vertx.rocketmq.client.impl.producer.TopicPublishInfo;
import io.vertx.rocketmq.client.log.ClientLogger;

public class MQAdminImpl {

    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private long timeoutMillis = 6000;

    public MQAdminImpl(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public void setTimeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public Future<Void> createTopic(String key, String newTopic, int queueNum) {
        return createTopic(key, newTopic, queueNum, 0);
    }

    public Future<Void> createTopic(String key, String newTopic, int queueNum, int topicSysFlag) {
        Promise<Void> promise = Promise.promise();
        try {
            Validators.checkTopic(newTopic);
            Validators.isSystemTopic(newTopic);
            Future<TopicRouteData> topicRouteDataFuture = this.mQClientFactory.getMQClientAPIImpl()
                    .getTopicRouteInfoFromNameServer(key, timeoutMillis);
            topicRouteDataFuture.onFailure(promise::fail);
            topicRouteDataFuture.onSuccess(topicRouteData -> {
                List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
                if (brokerDataList != null && !brokerDataList.isEmpty()) {
                    Collections.sort(brokerDataList);
                    for (BrokerData brokerData : brokerDataList) {
                        String addr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                        if (addr != null) {
                            TopicConfig topicConfig = new TopicConfig(newTopic);
                            topicConfig.setReadQueueNums(queueNum);
                            topicConfig.setWriteQueueNums(queueNum);
                            topicConfig.setTopicSysFlag(topicSysFlag);
                            this.mQClientFactory.getMQClientAPIImpl().createTopic(addr, key, topicConfig, timeoutMillis);
                        }
                    }
                } else {
                    promise.fail(new MQClientException("Not found broker, maybe key is wrong", null));
                }
            });

        } catch (Exception e) {
            promise.fail(new MQClientException("create new topic failed", e));
        }

        return promise.future();
    }

    public Future<List<MessageQueue>> fetchPublishMessageQueues(String topic) {
        Promise<List<MessageQueue>> promise = Promise.promise();
        Future<TopicRouteData> topicRouteDataFuture = this.mQClientFactory.getMQClientAPIImpl()
                .getTopicRouteInfoFromNameServer(topic, timeoutMillis);
        topicRouteDataFuture.onFailure(promise::fail);
        topicRouteDataFuture.onSuccess(topicRouteData -> {
            if (topicRouteData != null) {
                TopicPublishInfo topicPublishInfo = MQClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData);
                if (topicPublishInfo.ok()) {
                    promise.complete(parsePublishMessageQueues(topicPublishInfo.getMessageQueueList()));
                    return;
                }
            }
            promise.fail(new MQClientException("Unknow why, Can not find Message Queue for this topic, " + topic, null));
        });

        return promise.future();
    }

    public List<MessageQueue> parsePublishMessageQueues(List<MessageQueue> messageQueueList) {
        List<MessageQueue> resultQueues = new ArrayList<>();
        for (MessageQueue queue : messageQueueList) {
            String userTopic = NamespaceUtil.withoutNamespace(queue.getTopic(),
                    this.mQClientFactory.getClientConfig().getNamespace());
            resultQueues.add(new MessageQueue(userTopic, queue.getBrokerName(), queue.getQueueId()));
        }

        return resultQueues;
    }

    public Future<Set<MessageQueue>> fetchSubscribeMessageQueues(String topic) {
        Promise<Set<MessageQueue>> promise = Promise.promise();
        Future<TopicRouteData> topicRouteDataFuture = this.mQClientFactory.getMQClientAPIImpl()
                .getTopicRouteInfoFromNameServer(topic, timeoutMillis);
        topicRouteDataFuture.onFailure(e -> promise.fail(new MQClientException(
                "Can not find Message Queue for this topic, " + topic + FAQUrl.suggestTodo(FAQUrl.MQLIST_NOT_EXIST), e)));
        topicRouteDataFuture.onSuccess(topicRouteData -> {
            if (topicRouteData != null) {
                Set<MessageQueue> mqList = MQClientInstance.topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                if (!mqList.isEmpty()) {
                    promise.complete(mqList);
                } else {
                    promise.fail(new MQClientException(
                            "Can not find Message Queue for this topic, " + topic + " Namesrv return empty", null));
                }
            } else {
                promise.fail(new MQClientException("Unknow why, Can not find Message Queue for this topic, " + topic, null));
            }
        });

        return promise.future();
    }

    public Future<Long> searchOffset(MessageQueue mq, long timestamp) {

        Promise<Long> promise = Promise.promise();
        Promise<String> brokerAddrPromise = Promise.promise();
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            Future<Boolean> booleanFuture = this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            booleanFuture.onFailure(brokerAddrPromise::fail);
            booleanFuture.onSuccess(
                    bool -> brokerAddrPromise.complete(this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName())));
        } else {
            brokerAddrPromise.complete(brokerAddr);
        }

        brokerAddrPromise.future().onFailure(promise::fail);
        brokerAddrPromise.future().onSuccess(addr -> {
            if (addr != null) {
                Future<Long> offsetFuture = this.mQClientFactory.getMQClientAPIImpl().searchOffset(addr, mq.getTopic(),
                        mq.getQueueId(), timestamp,
                        timeoutMillis);
                offsetFuture.onFailure(e -> promise.fail(new MQClientException("Invoke Broker[" + addr + "] exception", e)));
                offsetFuture.onSuccess(promise::complete);
            } else {
                promise.fail(new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null));
            }
        });
        return promise.future();
    }

    public Future<Long> maxOffset(MessageQueue mq) {
        Promise<Long> promise = Promise.promise();
        Promise<String> brokerAddrPromise = Promise.promise();
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            Future<Boolean> booleanFuture = this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            booleanFuture.onFailure(brokerAddrPromise::fail);
            booleanFuture.onSuccess(
                    bool -> brokerAddrPromise.complete(this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName())));
        } else {
            brokerAddrPromise.complete(brokerAddr);
        }

        brokerAddrPromise.future().onFailure(promise::fail);
        brokerAddrPromise.future().onSuccess(addr -> {
            if (addr != null) {
                Future<Long> maxOffset = this.mQClientFactory.getMQClientAPIImpl().getMaxOffset(brokerAddr, mq.getTopic(),
                        mq.getQueueId(), timeoutMillis);
                maxOffset.onComplete(ar -> {
                    if (ar.failed()) {
                        promise.fail(new MQClientException("Invoke Broker[" + brokerAddr + "] exception", ar.cause()));
                    } else {
                        promise.complete(ar.result());
                    }
                });
            } else {
                promise.fail(new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null));
            }
        });

        return promise.future();
    }

    public Future<Long> minOffset(MessageQueue mq) {
        Promise<Long> promise = Promise.promise();
        Promise<String> promiseAddr = Promise.promise();
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            Future<Boolean> booleanFuture = this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            booleanFuture.onFailure(promiseAddr::fail);
            booleanFuture.onSuccess(
                    bool -> promiseAddr.complete(this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName())));
        } else {
            promiseAddr.complete(brokerAddr);
        }

        promiseAddr.future().onFailure(promise::fail).onSuccess(addr -> {
            if (addr != null) {
                this.mQClientFactory.getMQClientAPIImpl().getMinOffset(addr, mq.getTopic(), mq.getQueueId(), timeoutMillis)
                        .onFailure(e -> promise.fail(new MQClientException("Invoke Broker[" + addr + "] exception", e)))
                        .onSuccess(promise::complete);
            } else {
                promise.fail(new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null));
            }
        });

        return promise.future();
    }

    public Future<Long> earliestMsgStoreTime(MessageQueue mq) {
        Promise<Long> promise = Promise.promise();
        Promise<String> promiseAddr = Promise.promise();
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            Future<Boolean> booleanFuture = this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            booleanFuture.onFailure(promiseAddr::fail);
            booleanFuture.onSuccess(
                    bool -> promiseAddr.complete(this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName())));
        } else {
            promiseAddr.complete(brokerAddr);
        }

        promiseAddr.future().onFailure(promise::fail).onSuccess(addr -> {
            if (addr != null) {
                this.mQClientFactory.getMQClientAPIImpl().getEarliestMsgStoretime(addr, mq.getTopic(), mq.getQueueId(),
                        timeoutMillis)
                        .onFailure(e -> promise.fail(new MQClientException("Invoke Broker[" + addr + "] exception", e)))
                        .onSuccess(promise::complete);
            } else {
                promise.fail(new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null));
            }
        });
        return promise.future();
    }

    public Future<MessageExt> viewMessage(String msgId) {
        Promise<MessageExt> promise = Promise.promise();
        try {
            MessageId messageId = MessageDecoder.decodeMessageId(msgId);
            this.mQClientFactory.getMQClientAPIImpl().viewMessage(RemotingUtil.socketAddress2String(messageId.getAddress()),
                    messageId.getOffset(), timeoutMillis).onFailure(promise::fail).onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(new MQClientException(ResponseCode.NO_MESSAGE, "query message by id finished, but no message."));
        }
        return promise.future();
    }

    public Future<QueryResult> queryMessage(String topic, String key, int maxNum, long begin, long end) {
        return queryMessage(topic, key, maxNum, begin, end, false);
    }

    public Future<MessageExt> queryMessageByUniqKey(String topic, String uniqKey) {

        Promise<MessageExt> promise = Promise.promise();
        Future<QueryResult> qrFuture = this.queryMessage(topic, uniqKey, 32,
                MessageClientIDSetter.getNearlyTimeFromID(uniqKey).getTime() - 1000, Long.MAX_VALUE, true);
        qrFuture.onFailure(promise::fail);
        qrFuture.onSuccess(qr -> {
            if (qr != null && qr.getMessageList() != null && qr.getMessageList().size() > 0) {
                promise.complete(qr.getMessageList().get(0));
            } else {
                promise.complete();
            }
        });
        return promise.future();
    }

    protected Future<QueryResult> queryMessage(String topic, String key, int maxNum, long begin, long end, boolean isUniqKey) {
        Promise<QueryResult> promise = Promise.promise();
        Promise<Object> routeDataPromise = Promise.promise();
        TopicRouteData topicRouteData = this.mQClientFactory.getAnExistTopicRouteData(topic);
        if (null == topicRouteData) {
            Future<Boolean> booleanFuture = this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            booleanFuture.onFailure(routeDataPromise::fail);
            booleanFuture.onSuccess(bool -> routeDataPromise.complete(this.mQClientFactory.getAnExistTopicRouteData(topic)));
        } else {
            routeDataPromise.complete(topicRouteData);
        }

        if (topicRouteData != null) {
            List<String> brokerAddrs = new LinkedList<>();
            for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                String addr = brokerData.selectBrokerAddr();
                if (addr != null) {
                    brokerAddrs.add(addr);
                }
            }

            if (!brokerAddrs.isEmpty()) {

                List<Future<QueryResult>> resultList = new ArrayList<>();
                for (String addr : brokerAddrs) {
                    QueryMessageRequestHeader requestHeader = new QueryMessageRequestHeader();
                    requestHeader.setTopic(topic);
                    requestHeader.setKey(key);
                    requestHeader.setMaxNum(maxNum);
                    requestHeader.setBeginTimestamp(begin);
                    requestHeader.setEndTimestamp(end);

                    Future<QueryResult> resultFuture = this.mQClientFactory.getMQClientAPIImpl().queryMessage(addr,
                            requestHeader, timeoutMillis * 3, isUniqKey);
                    resultList.add(resultFuture);
                }
                CompositeFutureImpl.any(resultList.toArray(new Future[0])).onComplete(ar -> {
                    List<MessageExt> messageList = new LinkedList<>();
                    List<QueryResult> queryResultList = ar.result().list();
                    long indexLastUpdateTimestamp = 0;
                    for (QueryResult qr : queryResultList) {
                        if (qr.getIndexLastUpdateTimestamp() > indexLastUpdateTimestamp) {
                            indexLastUpdateTimestamp = qr.getIndexLastUpdateTimestamp();
                        }

                        for (MessageExt msgExt : qr.getMessageList()) {
                            if (isUniqKey) {
                                if (msgExt.getMsgId().equals(key)) {

                                    if (messageList.size() > 0) {

                                        if (messageList.get(0).getStoreTimestamp() > msgExt.getStoreTimestamp()) {

                                            messageList.clear();
                                            messageList.add(msgExt);
                                        }
                                    } else {
                                        messageList.add(msgExt);
                                    }
                                } else {
                                    log.warn("queryMessage by uniqKey, find message key not matched, maybe hash duplicate {}",
                                            msgExt.toString());
                                }
                            } else {
                                String keys = msgExt.getKeys();
                                if (keys != null) {
                                    boolean matched = false;
                                    String[] keyArray = keys.split(MessageConst.KEY_SEPARATOR);
                                    for (String k : keyArray) {
                                        if (key.equals(k)) {
                                            matched = true;
                                            break;
                                        }
                                    }
                                    if (matched) {
                                        messageList.add(msgExt);
                                    } else {
                                        log.warn("queryMessage, find message key not matched, maybe hash duplicate {}",
                                                msgExt.toString());
                                    }
                                }
                            }
                        }

                        //If namespace not null , reset Topic without namespace.
                        for (MessageExt messageExt : messageList) {
                            if (null != this.mQClientFactory.getClientConfig().getNamespace()) {
                                messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(),
                                        this.mQClientFactory.getClientConfig().getNamespace()));
                            }
                        }

                        if (!messageList.isEmpty()) {
                            promise.complete(new QueryResult(indexLastUpdateTimestamp, messageList));
                        } else {
                            promise.fail(new MQClientException(ResponseCode.NO_MESSAGE,
                                    "query message by key finished, but no message."));
                        }
                    }
                });

            }
        } else {
            promise.fail(
                    new MQClientException(ResponseCode.TOPIC_NOT_EXIST, "The topic[" + topic + "] not matched route info"));
        }

        return promise.future();
    }

}
