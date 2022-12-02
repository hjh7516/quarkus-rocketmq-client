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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.VertxInternal;
import io.vertx.rocketmq.client.consumer.PullResult;
import io.vertx.rocketmq.client.consumer.PullStatus;
import io.vertx.rocketmq.client.exception.MQClientException;
import io.vertx.rocketmq.client.hook.FilterMessageContext;
import io.vertx.rocketmq.client.hook.FilterMessageHook;
import io.vertx.rocketmq.client.impl.factory.MQClientInstance;
import io.vertx.rocketmq.client.log.ClientLogger;

public class PullAPIWrapper {
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private final String consumerGroup;
    private final boolean unitMode;
    private final ConcurrentMap<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable = new ConcurrentHashMap<>(32);
    private volatile boolean connectBrokerByUser = false;
    private volatile long defaultBrokerId = MixAll.MASTER_ID;
    private final Random random = new Random(System.currentTimeMillis());
    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<>();
    private final VertxInternal vertx;

    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode, VertxInternal vertx) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
        this.vertx = vertx;
    }

    public Future<PullResultExt> processPullResult(final MessageQueue mq, final PullResult pullResult,
            final SubscriptionData subscriptionData) {

        PullResultExt pullResultExt = (PullResultExt) pullResult;
        Promise<PullResultExt> promise = Promise.promise();
        promise.complete((PullResultExt) pullResult);
        Future<PullResultExt> extFuture = promise.future();

        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());
        if (PullStatus.FOUND == pullResult.getPullStatus()) {
            extFuture = extFuture.compose(v -> {
                ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
                List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

                List<MessageExt> msgListFilterAgain = msgList;
                if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                    msgListFilterAgain = new ArrayList<>(msgList.size());
                    for (MessageExt msg : msgList) {
                        if (msg.getTags() != null) {
                            if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                                msgListFilterAgain.add(msg);
                            }
                        }
                    }
                }

                return this.executeHook(msgListFilterAgain);
            }).map(msgListFilterAgain -> {
                for (MessageExt msg : msgListFilterAgain) {
                    String traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                    if (Boolean.parseBoolean(traFlag)) {
                        msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                    }
                    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET,
                            Long.toString(pullResult.getMinOffset()));
                    MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET,
                            Long.toString(pullResult.getMaxOffset()));
                    msg.setBrokerName(mq.getBrokerName());
                }
                pullResultExt.setMsgFoundList(msgListFilterAgain);
                return pullResultExt;
            });
        }
        extFuture.onComplete(ar -> pullResultExt.setMessageBinary(null));
        return extFuture;
    }

    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }

    public Future<List<MessageExt>> executeHook(List<MessageExt> msgListFilterAgain) {
        return vertx.executeBlocking(blockPromis -> {
            if (!this.filterMessageHookList.isEmpty()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                for (FilterMessageHook hook : this.filterMessageHookList) {
                    try {
                        hook.filterMessage(filterMessageContext);
                    } catch (Throwable e) {
                        log.error("execute hook error. hookName={}", hook.hookName());
                    }
                }
            }

            blockPromis.complete(msgListFilterAgain);
        });
    }

    public Future<PullResult> pullKernelImpl(
            final MessageQueue mq,
            final String subExpression,
            final String expressionType,
            final long subVersion,
            final long offset,
            final int maxNums,
            final int sysFlag,
            final long commitOffset,
            final long brokerSuspendMaxTimeMillis,
            final long timeoutMillis) {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                this.recalculatePullFromWhichNode(mq), false);
        Promise<FindBrokerResult> promise = Promise.promise();
        if (null == findBrokerResult) {
            Future<Boolean> booleanFuture = this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            booleanFuture.onFailure(promise::fail);
            booleanFuture
                    .onSuccess(bool -> promise.complete(this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                            this.recalculatePullFromWhichNode(mq), false)));
        } else {
            promise.complete(findBrokerResult);
        }

        return promise.future().compose(brokerResult -> {
            Promise<PullResult> innerPromise = Promise.promise();
            if (brokerResult != null) {
                {
                    if (!ExpressionType.isTagType(expressionType)
                            && brokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                        innerPromise.fail(new MQClientException("The broker[" + mq.getBrokerName() + ", "
                                + brokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by "
                                + expressionType, null));
                        return innerPromise.future();
                    }
                }
                int sysFlagInner = sysFlag;
                if (brokerResult.isSlave()) {
                    sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
                }
                PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
                requestHeader.setConsumerGroup(this.consumerGroup);
                requestHeader.setTopic(mq.getTopic());
                requestHeader.setQueueId(mq.getQueueId());
                requestHeader.setQueueOffset(offset);
                requestHeader.setMaxMsgNums(maxNums);
                requestHeader.setSysFlag(sysFlagInner);
                requestHeader.setCommitOffset(commitOffset);
                requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
                requestHeader.setSubscription(subExpression);
                requestHeader.setSubVersion(subVersion);
                requestHeader.setExpressionType(expressionType);

                String brokerAddr = brokerResult.getBrokerAddr();
                if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
                    try {
                        brokerAddr = computPullFromWhichFilterServer(mq.getTopic(), brokerAddr);
                    } catch (MQClientException e) {
                        innerPromise.fail(e);
                        return innerPromise.future();
                    }
                }
                this.mQClientFactory.getMQClientAPIImpl().pullMessage(
                        brokerAddr,
                        requestHeader,
                        timeoutMillis).onComplete(ar -> {
                            if (ar.failed()) {
                                innerPromise.fail(ar.cause());
                            } else {
                                innerPromise.complete(ar.result());
                            }
                        });
            } else {
                innerPromise.fail(new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null));
            }

            return innerPromise.future();
        });
    }

    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        if (this.isConnectBrokerByUser()) {
            return this.defaultBrokerId;
        }

        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }

        return MixAll.MASTER_ID;
    }

    private String computPullFromWhichFilterServer(final String topic, final String brokerAddr)
            throws MQClientException {
        ConcurrentMap<String, TopicRouteData> topicRouteTable = this.mQClientFactory.getTopicRouteTable();
        if (topicRouteTable != null) {
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);

            if (list != null && !list.isEmpty()) {
                return list.get(randomNum() % list.size());
            }
        }

        throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: "
                + topic, null);
    }

    public boolean isConnectBrokerByUser() {
        return connectBrokerByUser;
    }

    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.connectBrokerByUser = connectBrokerByUser;

    }

    public int randomNum() {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
            if (value < 0)
                value = 0;
        }
        return value;
    }

    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
        this.filterMessageHookList = filterMessageHookList;
    }

    public long getDefaultBrokerId() {
        return defaultBrokerId;
    }

    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultBrokerId = defaultBrokerId;
    }
}
