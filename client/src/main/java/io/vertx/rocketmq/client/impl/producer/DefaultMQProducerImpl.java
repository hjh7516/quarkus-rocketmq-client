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
package io.vertx.rocketmq.client.impl.producer;

import static org.apache.rocketmq.common.ServiceState.RUNNING;
import static org.apache.rocketmq.common.ServiceState.SHUTDOWN_ALREADY;
import static org.apache.rocketmq.common.ServiceState.START_FAILED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.CorrelationIdUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.impl.VertxInternal;
import io.vertx.rocketmq.client.QueryResult;
import io.vertx.rocketmq.client.ReactiveUtil;
import io.vertx.rocketmq.client.Validators;
import io.vertx.rocketmq.client.common.ClientErrorCode;
import io.vertx.rocketmq.client.common.RocketmqOptions;
import io.vertx.rocketmq.client.exception.MQClientException;
import io.vertx.rocketmq.client.exception.RequestTimeoutException;
import io.vertx.rocketmq.client.hook.CheckForbiddenContext;
import io.vertx.rocketmq.client.hook.CheckForbiddenHook;
import io.vertx.rocketmq.client.hook.SendMessageContext;
import io.vertx.rocketmq.client.hook.SendMessageHook;
import io.vertx.rocketmq.client.impl.MQClientManager;
import io.vertx.rocketmq.client.impl.factory.MQClientInstance;
import io.vertx.rocketmq.client.latency.MQFaultStrategy;
import io.vertx.rocketmq.client.log.ClientLogger;
import io.vertx.rocketmq.client.producer.DefaultMQProducer;
import io.vertx.rocketmq.client.producer.LocalTransactionExecuter;
import io.vertx.rocketmq.client.producer.LocalTransactionState;
import io.vertx.rocketmq.client.producer.MessageQueueSelector;
import io.vertx.rocketmq.client.producer.RequestFutureTable;
import io.vertx.rocketmq.client.producer.RequestResponseFuture;
import io.vertx.rocketmq.client.producer.SendResult;
import io.vertx.rocketmq.client.producer.SendStatus;
import io.vertx.rocketmq.client.producer.TransactionCheckListener;
import io.vertx.rocketmq.client.producer.TransactionListener;
import io.vertx.rocketmq.client.producer.TransactionMQProducer;
import io.vertx.rocketmq.client.producer.TransactionSendResult;
import io.vertx.rocketmq.client.producer.consts.CommunicationMode;

public class DefaultMQProducerImpl implements MQProducerInner {
    private final InternalLogger log = ClientLogger.getLog();
    private final DefaultMQProducer defaultMQProducer;
    private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable = new ConcurrentHashMap<>();
    private final ArrayList<SendMessageHook> sendMessageHookList = new ArrayList<>();
    private final Timer timer = new Timer("RequestHouseKeepingService", true);
    private AtomicReference<ServiceState> serviceState = new AtomicReference<>(ServiceState.CREATE_JUST);
    private MQClientInstance mQClientFactory;
    private final ArrayList<CheckForbiddenHook> checkForbiddenHookList = new ArrayList<>();
    private int zipCompressLevel = Integer.parseInt(System.getProperty(MixAll.MESSAGE_COMPRESS_LEVEL, "5"));
    private final MQFaultStrategy mqFaultStrategy = new MQFaultStrategy();
    private final VertxInternal vertx;
    private final RocketmqOptions options;

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer, VertxInternal vertx, RocketmqOptions options) {
        this.defaultMQProducer = defaultMQProducer;
        this.vertx = vertx;
        this.options = options;
    }

    public void registerCheckForbiddenHook(CheckForbiddenHook checkForbiddenHook) {
        this.checkForbiddenHookList.add(checkForbiddenHook);
        log.info("register a new checkForbiddenHook. hookName={}, allHookSize={}", checkForbiddenHook.hookName(),
                checkForbiddenHookList.size());
    }

    public void initTransactionEnv() {
    }

    public void destroyTransactionEnv() {
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register sendMessage Hook, {}", hook.hookName());
    }

    public void start() throws MQClientException {
        this.start(true);
    }

    public void start(final boolean startFactory) throws MQClientException {

        if (this.serviceState.compareAndSet(ServiceState.CREATE_JUST, START_FAILED)) {
            this.checkConfig();
            if (!this.options.getGroupName().equals(MixAll.CLIENT_INNER_PRODUCER_GROUP)) {
                this.options.changeInstanceNameToPID();
            }
            this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(vertx, options);
            boolean registerOK = mQClientFactory.registerProducer(this.options.getGroupName(), this);
            if (!registerOK) {
                this.serviceState.set(ServiceState.CREATE_JUST);
                throw new MQClientException("The producer group[" + this.options.getGroupName()
                        + "] has been created before, specify another name please."
                        + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                        null);
            }

            this.topicPublishInfoTable.put(this.defaultMQProducer.getCreateTopicKey(), new TopicPublishInfo());
            if (startFactory) {
                mQClientFactory.start();
            }

            log.info("the producer [{}] start OK. sendMessageWithVIPChannel={}", this.defaultMQProducer.getProducerGroup(),
                    this.defaultMQProducer.isSendMessageWithVIPChannel());
            this.serviceState.set(RUNNING);
        } else if (RUNNING.equals(this.serviceState.get())
                || START_FAILED.equals(this.serviceState.get())
                || SHUTDOWN_ALREADY.equals(this.serviceState.get())) {
            throw new MQClientException("The producer service state not OK, maybe started once, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
        }

        ReactiveUtil.waitOne(this.mQClientFactory.sendHeartbeatToAllBrokerWithLock());
    }

    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQProducer.getProducerGroup());

        if (null == this.defaultMQProducer.getProducerGroup()) {
            throw new MQClientException("producerGroup is null", null);
        }

        if (this.defaultMQProducer.getProducerGroup().equals(MixAll.DEFAULT_PRODUCER_GROUP)) {
            throw new MQClientException(
                    "producerGroup can not equal " + MixAll.DEFAULT_PRODUCER_GROUP + ", please specify another one.",
                    null);
        }
    }

    public Future<Void> shutdown(boolean shutdownFactory) {
        if (this.serviceState.compareAndSet(RUNNING, SHUTDOWN_ALREADY)) {
            Future<Void> voidFuture = this.mQClientFactory.unregisterProducer(this.defaultMQProducer.getProducerGroup());
            if (shutdownFactory) {
                voidFuture.compose(v -> this.mQClientFactory.shutdown());
            }

            voidFuture.onSuccess(v -> {
                this.timer.cancel();
                log.info("the producer [{}] shutdown OK", this.defaultMQProducer.getProducerGroup());
            });
            return voidFuture;
        }

        return Future.succeededFuture();
    }

    @Override
    public Set<String> getPublishTopicList() {
        return new HashSet<>(this.topicPublishInfoTable.keySet());
    }

    @Override
    public boolean isPublishTopicNeedUpdate(String topic) {
        TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);

        return null == prev || !prev.ok();
    }

    /**
     * This method will be removed in the version 5.0.0 and <code>getCheckListener</code> is recommended.
     */
    @Override
    @Deprecated
    public TransactionCheckListener checkListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionCheckListener();
        }

        return null;
    }

    @Override
    public TransactionListener getCheckListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionListener();
        }
        return null;
    }

    @Override
    public void checkTransactionState(final String addr, final MessageExt msg,
            final CheckTransactionStateRequestHeader header) {
        String group = DefaultMQProducerImpl.this.defaultMQProducer.getProducerGroup();
        TransactionCheckListener transactionCheckListener = DefaultMQProducerImpl.this.checkListener();
        TransactionListener transactionListener = getCheckListener();
        if (transactionCheckListener != null || transactionListener != null) {
            Future<LocalTransactionState> stateFuture;
            if (transactionCheckListener != null) {
                stateFuture = transactionCheckListener.checkLocalTransactionState(msg);
            } else {
                log.debug("Used new check API in transaction message");
                stateFuture = transactionListener.checkLocalTransaction(msg);
            }

            stateFuture.onComplete(ar -> {
                LocalTransactionState localTransactionState = ar.result();
                if (ar.failed()) {
                    log.warn("CheckTransactionState, pick transactionCheckListener by group[{}] failed", group);
                    localTransactionState = LocalTransactionState.UNKNOW;
                }
                this.processTransactionState(addr, localTransactionState, group, ar.cause(), header, msg);
            });
        } else {
            log.warn("CheckTransactionState, pick transactionCheckListener by group[{}] failed", group);
        }
    }

    private void processTransactionState(String brokerAddr,
            final LocalTransactionState localTransactionState,
            final String producerGroup,
            final Throwable exception, CheckTransactionStateRequestHeader header, MessageExt message) {
        final EndTransactionRequestHeader thisHeader = new EndTransactionRequestHeader();
        thisHeader.setCommitLogOffset(header.getCommitLogOffset());
        thisHeader.setProducerGroup(producerGroup);
        thisHeader.setTranStateTableOffset(header.getTranStateTableOffset());
        thisHeader.setFromTransactionCheck(true);

        String uniqueKey = message.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        if (uniqueKey == null) {
            uniqueKey = message.getMsgId();
        }
        thisHeader.setMsgId(uniqueKey);
        thisHeader.setTransactionId(header.getTransactionId());
        switch (localTransactionState) {
            case COMMIT_MESSAGE:
                thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                break;
            case ROLLBACK_MESSAGE:
                thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                log.warn("when broker check, client rollback this transaction, {}", thisHeader);
                break;
            case UNKNOW:
                thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                log.warn("when broker check, client does not know this transaction state, {}", thisHeader);
                break;
            default:
                break;
        }

        String remark = null;
        if (exception != null) {
            remark = "checkLocalTransactionState Exception: " + RemotingHelper.exceptionSimpleDesc(exception);
        }

        DefaultMQProducerImpl.this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, thisHeader, remark,
                3000).onFailure(e -> log.error("endTransactionOneway exception", e));
    }

    @Override
    public void updateTopicPublishInfo(final String topic, final TopicPublishInfo info) {
        if (info != null && topic != null) {
            TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
            if (prev != null) {
                log.info("updateTopicPublishInfo prev is not null, " + prev);
            }
        }
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQProducer.isUnitMode();
    }

    public Future<Void> createTopic(String key, String newTopic, int queueNum) {
        return createTopic(key, newTopic, queueNum, 0);
    }

    public Future<Void> createTopic(String key, String newTopic, int queueNum, int topicSysFlag) {

        Promise<Void> promise = Promise.promise();
        try {
            this.makeSureStateOK();
            Validators.checkTopic(newTopic);
            Validators.isSystemTopic(newTopic);
            this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag)
                    .onFailure(promise::fail).onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }
        return promise.future();
    }

    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState.get() != RUNNING) {
            throw new MQClientException("The producer service state not OK, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
        }
    }

    public Future<List<MessageQueue>> fetchPublishMessageQueues(String topic) {
        Promise<List<MessageQueue>> promise = Promise.promise();
        try {
            this.makeSureStateOK();
            this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic)
                    .onFailure(promise::fail).onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }

        return promise.future();
    }

    public Future<Long> searchOffset(MessageQueue mq, long timestamp) {
        Promise<Long> promise = Promise.promise();
        try {
            this.makeSureStateOK();
            this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp)
                    .onFailure(promise::fail).onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }
        return promise.future();
    }

    public Future<Long> maxOffset(MessageQueue mq) {
        Promise<Long> promise = Promise.promise();
        try {
            this.makeSureStateOK();
            this.mQClientFactory.getMQAdminImpl().maxOffset(mq)
                    .onFailure(promise::fail).onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }
        return promise.future();
    }

    public Future<Long> minOffset(MessageQueue mq) {
        Promise<Long> promise = Promise.promise();
        try {
            this.makeSureStateOK();
            this.mQClientFactory.getMQAdminImpl().minOffset(mq).onFailure(promise::fail).onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }
        return promise.future();
    }

    public Future<Long> earliestMsgStoreTime(MessageQueue mq) {
        Promise<Long> promise = Promise.promise();
        try {
            this.makeSureStateOK();
            this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq).onFailure(promise::fail)
                    .onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }

        return promise.future();
    }

    public Future<MessageExt> viewMessage(String msgId) {
        Promise<MessageExt> promise = Promise.promise();
        try {
            this.makeSureStateOK();
            this.mQClientFactory.getMQAdminImpl().viewMessage(msgId).onFailure(promise::fail).onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }
        return promise.future();
    }

    public Future<QueryResult> queryMessage(String topic, String key, int maxNum, long begin, long end) {
        Promise<QueryResult> promise = Promise.promise();
        try {
            this.makeSureStateOK();
            this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end)
                    .onFailure(promise::fail).onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }
        return promise.future();
    }

    public Future<MessageExt> queryMessageByUniqKey(String topic, String uniqKey) {
        Promise<MessageExt> promise = Promise.promise();
        try {
            this.makeSureStateOK();
            this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey)
                    .onFailure(promise::fail).onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }
        return promise.future();
    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        return this.mqFaultStrategy.selectOneMessageQueue(tpInfo, lastBrokerName);
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        this.mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation);
    }

    private void validateNameServerSetting() throws MQClientException {
        List<String> nsList = this.getmQClientFactory().getMQClientAPIImpl().getNameServerAddressList();
        if (null == nsList || nsList.isEmpty()) {
            throw new MQClientException(
                    "No name server address, please set it." + FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL), null)
                            .setResponseCode(ClientErrorCode.NO_NAME_SERVER_EXCEPTION);
        }

    }

    private void sendDefaultImpl(
            Message msg,
            final CommunicationMode communicationMode, TopicPublishInfo topicPublishInfo,
            final long timeout, int remainTimes, Promise<SendResult> promise, String lastBrokerName) {
        long beginTimestampFirst = System.currentTimeMillis();
        long beginTimestampPrev;
        MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName);
        if (mqSelected == null) {
            promise.fail(new RemotingTooMuchRequestException("Unkown MQ"));
            return;
        }
        beginTimestampPrev = System.currentTimeMillis();
        long costTime = beginTimestampPrev - beginTimestampFirst;
        if (timeout < costTime) {
            promise.fail(new RemotingTooMuchRequestException("sendDefaultImpl call timeout"));
            return;
        }
        Future<SendResult> resultFuture;
        Handler<AsyncResult<SendResult>> handler;
        long finalBeginTimestampPrev = beginTimestampPrev;
        if (CommunicationMode.ONEWAY.equals(communicationMode)) {
            resultFuture = this.sendKernelImpl(msg, mqSelected, communicationMode, timeout - costTime);
            handler = ar -> {
                if (ar.failed()) {
                    this.updateFaultItem(mqSelected.getBrokerName(), System.currentTimeMillis() - finalBeginTimestampPrev,
                            true);
                    if (remainTimes > 0) {
                        sendDefaultImpl(msg, communicationMode, topicPublishInfo, timeout, remainTimes - 1, promise,
                                mqSelected.getBrokerName());
                    } else {
                        promise.fail(ar.cause());
                    }
                } else {
                    this.updateFaultItem(mqSelected.getBrokerName(), System.currentTimeMillis() - finalBeginTimestampPrev,
                            false);
                    promise.complete();
                }
            };
        } else {
            resultFuture = this.sendKernelImpl(msg, mqSelected, communicationMode, timeout - costTime);
            handler = ar -> {
                if (ar.failed()) {
                    this.updateFaultItem(mqSelected.getBrokerName(), System.currentTimeMillis() - finalBeginTimestampPrev,
                            true);
                    if (remainTimes > 0) {
                        sendDefaultImpl(msg, communicationMode, topicPublishInfo, timeout, remainTimes - 1, promise,
                                mqSelected.getBrokerName());
                    } else {
                        promise.fail(ar.cause());
                    }
                } else {
                    this.updateFaultItem(mqSelected.getBrokerName(), System.currentTimeMillis() - finalBeginTimestampPrev,
                            false);
                    if (ar.result().getSendStatus() != SendStatus.SEND_OK) {
                        if (this.defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK() && remainTimes > 0) {
                            sendDefaultImpl(msg, communicationMode, topicPublishInfo, timeout, remainTimes - 1, promise,
                                    mqSelected.getBrokerName());
                        } else {
                            promise.complete(ar.result());
                        }
                    } else {
                        promise.complete(ar.result());
                    }
                }
            };
        }
        resultFuture.onComplete(handler);
    }

    private Future<TopicPublishInfo> tryToFindTopicPublishInfo(final String topic) {
        Promise<TopicPublishInfo> promise = Promise.promise();
        TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
        if (null != topicPublishInfo && topicPublishInfo.isHaveTopicRouterInfo() && topicPublishInfo.ok()) {
            promise.complete(topicPublishInfo);
            return promise.future();
        }

        this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
        Future<Boolean> future = this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
        future.onFailure(promise::fail);
        future.onSuccess(bool -> {
            TopicPublishInfo againGet = this.topicPublishInfoTable.get(topic);
            if (againGet.isHaveTopicRouterInfo() || againGet.ok()) {
                promise.complete(againGet);
            } else {
                Future<Boolean> defalutFuture = this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic, true,
                        this.defaultMQProducer);
                defalutFuture.onFailure(promise::fail);
                defalutFuture.onSuccess(b -> promise.complete(this.topicPublishInfoTable.get(topic)));
            }
        });

        return promise.future();
    }

    private Future<SendResult> sendKernelImpl(final Message msg,
            final MessageQueue mq,
            final CommunicationMode communicationMode,
            final long timeout) {
        long beginStartTime = System.currentTimeMillis();
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            tryToFindTopicPublishInfo(mq.getTopic());
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }
        if (brokerAddr != null) {
            brokerAddr = MixAll.brokerVIPChannel(this.defaultMQProducer.isSendMessageWithVIPChannel(), brokerAddr);

            byte[] prevBody = msg.getBody();
            //for MessageBatch,ID has been set in the generating process
            if (!(msg instanceof MessageBatch)) {
                MessageClientIDSetter.setUniqID(msg);
            }

            if (null != this.mQClientFactory.getClientConfig().getNamespace()) {
                msg.setInstanceId(this.mQClientFactory.getClientConfig().getNamespace());
            }

            int sysFlag = 0;
            if (this.tryToCompressMessage(msg)) {
                sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
            }

            final String tranMsg = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
            if (Boolean.parseBoolean(tranMsg)) {
                sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
            }

            Promise<SendResult> promise = Promise.promise();

            String finalBrokerAddr = brokerAddr;
            int finalSysFlag = sysFlag;
            this.executeCheckForbiddenHook(msg, mq, communicationMode, brokerAddr)
                    .compose(
                            v -> this.executeSendMessageHookBefore(prepareContext(msg, mq, communicationMode, finalBrokerAddr)))
                    .compose(v -> {
                        SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
                        requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                        requestHeader.setTopic(msg.getTopic());
                        requestHeader.setDefaultTopic(this.defaultMQProducer.getCreateTopicKey());
                        requestHeader.setDefaultTopicQueueNums(this.defaultMQProducer.getDefaultTopicQueueNums());
                        requestHeader.setQueueId(mq.getQueueId());
                        requestHeader.setSysFlag(finalSysFlag);
                        requestHeader.setBornTimestamp(System.currentTimeMillis());
                        requestHeader.setFlag(msg.getFlag());
                        requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
                        requestHeader.setReconsumeTimes(0);
                        requestHeader.setUnitMode(this.isUnitMode());
                        requestHeader.setBatch(msg instanceof MessageBatch);
                        if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                            String reconsumeTimes = MessageAccessor.getReconsumeTime(msg);
                            if (reconsumeTimes != null) {
                                requestHeader.setReconsumeTimes(Integer.valueOf(reconsumeTimes));
                                MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME);
                            }

                            String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(msg);
                            if (maxReconsumeTimes != null) {
                                requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
                                MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
                            }
                        }

                        long costTimeSync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeSync) {
                            promise.fail(new RemotingTooMuchRequestException("sendKernelImpl call timeout"));
                            return promise.future();
                        }
                        return this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                                finalBrokerAddr,
                                mq.getBrokerName(),
                                msg,
                                requestHeader,
                                timeout - costTimeSync,
                                communicationMode);
                    }).onComplete(ar -> {
                        SendMessageContext context = prepareContext(msg, mq, communicationMode, finalBrokerAddr);
                        if (ar.failed()) {
                            context.setException(ar.cause());
                            this.executeSendMessageHookAfter(context).onComplete(e -> promise.fail(ar.cause()));
                        } else {
                            context.setSendResult(ar.result());
                            this.executeSendMessageHookAfter(context).onComplete(v -> promise.complete(ar.result()));
                        }
                        msg.setBody(prevBody);
                        msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
                    });

            return promise.future();
        }

        Promise<SendResult> promise = Promise.promise();
        promise.fail(new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null));
        return promise.future();
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    private boolean tryToCompressMessage(final Message msg) {
        if (msg instanceof MessageBatch) {
            //batch dose not support compressing right now
            return false;
        }
        byte[] body = msg.getBody();
        if (body != null) {
            if (body.length >= this.defaultMQProducer.getCompressMsgBodyOverHowmuch()) {
                try {
                    byte[] data = UtilAll.compress(body, zipCompressLevel);
                    if (data != null) {
                        msg.setBody(data);
                        return true;
                    }
                } catch (IOException e) {
                    log.error("tryToCompressMessage exception", e);
                    log.warn(msg.toString());
                }
            }
        }

        return false;
    }

    public boolean hasCheckForbiddenHook() {
        return !checkForbiddenHookList.isEmpty();
    }

    public Future<Void> executeCheckForbiddenHook(final Message msg,
            final MessageQueue mq,
            final CommunicationMode communicationMode, String brokerAddr) {

        CheckForbiddenContext checkForbiddenContext = new CheckForbiddenContext();
        checkForbiddenContext.setNameSrvAddr(this.defaultMQProducer.getNamesrvAddr());
        checkForbiddenContext.setGroup(this.defaultMQProducer.getProducerGroup());
        checkForbiddenContext.setCommunicationMode(communicationMode);
        checkForbiddenContext.setBrokerAddr(brokerAddr);
        checkForbiddenContext.setMessage(msg);
        checkForbiddenContext.setMq(mq);
        checkForbiddenContext.setUnitMode(this.isUnitMode());
        return vertx.executeBlocking(blockPromis -> {
            try {
                if (hasCheckForbiddenHook()) {
                    for (CheckForbiddenHook hook : checkForbiddenHookList) {
                        hook.checkForbidden(checkForbiddenContext);
                    }
                }
                blockPromis.complete();
            } catch (Exception e) {
                blockPromis.fail(e);
            }
        });
    }

    private SendMessageContext prepareContext(final Message msg,
            final MessageQueue mq,
            final CommunicationMode communicationMode, String brokerAddr) {
        SendMessageContext context = new SendMessageContext();
        context.setProducer(this);
        context.setProducerGroup(this.defaultMQProducer.getProducerGroup());
        context.setCommunicationMode(communicationMode);
        context.setBornHost(this.defaultMQProducer.getClientIP());
        context.setBrokerAddr(brokerAddr);
        context.setMessage(msg);
        context.setMq(mq);
        context.setNamespace(this.defaultMQProducer.getNamespace());
        String isTrans = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        if (isTrans != null && isTrans.equals("true")) {
            context.setMsgType(MessageType.Trans_Msg_Half);
        }

        if (msg.getProperty("__STARTDELIVERTIME") != null || msg.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null) {
            context.setMsgType(MessageType.Delay_Msg);
        }

        return context;
    }

    public Future<Void> executeSendMessageHookBefore(SendMessageContext context) {
        return vertx.executeBlocking(blockPromis -> {
            if (!this.sendMessageHookList.isEmpty()) {
                for (SendMessageHook hook : this.sendMessageHookList) {
                    try {
                        hook.sendMessageBefore(context);
                    } catch (Throwable e) {
                        log.warn("failed to executeSendMessageHookBefore", e);
                    }
                }
            }
            blockPromis.complete();
        });
    }

    public Future<Void> executeSendMessageHookAfter(SendMessageContext context) {
        return vertx.executeBlocking(blockPromis -> {
            if (!this.sendMessageHookList.isEmpty()) {
                for (SendMessageHook hook : this.sendMessageHookList) {
                    try {
                        hook.sendMessageAfter(context);
                    } catch (Throwable e) {
                        log.warn("failed to executeSendMessageHookAfter", e);
                    }
                }
            }
            blockPromis.complete();
        });
    }

    /**
     * DEFAULT ONEWAY -------------------------------------------------------
     */
    public Future<Void> sendOneway(Message msg) {
        Promise<Void> promise = Promise.promise();
        Promise<SendResult> tmpPromise = Promise.promise();
        Future<TopicPublishInfo> topicPublishInfoFuture = this.tryToFindTopicPublishInfo(msg.getTopic());
        topicPublishInfoFuture.onFailure(promise::fail);
        topicPublishInfoFuture.onSuccess(topicPublishInfo -> {
            try {
                if (topicPublishInfo != null && topicPublishInfo.ok()) {
                    this.sendDefaultImpl(msg, CommunicationMode.SYNC, topicPublishInfo,
                            this.defaultMQProducer.getSendMsgTimeout(), 1, tmpPromise, null);
                } else {
                    validateNameServerSetting();
                    promise.fail(new MQClientException(
                            "No route info of this topic: " + msg.getTopic() + FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO),
                            null).setResponseCode(ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION));
                }
            } catch (Exception e) {
                promise.fail(e);
            }
        });
        tmpPromise.future().onFailure(promise::fail);
        tmpPromise.future().onSuccess(r -> promise.complete());
        return promise.future();
    }

    /**
     * KERNEL SYNC -------------------------------------------------------
     */
    public Future<SendResult> send(Message msg, MessageQueue mq) {
        return send(msg, mq, this.defaultMQProducer.getSendMsgTimeout());
    }

    public Future<SendResult> send(Message msg, MessageQueue mq, long timeout) {
        long beginStartTime = System.currentTimeMillis();
        Promise<SendResult> promise = Promise.promise();
        try {
            this.makeSureStateOK();
            Validators.checkMessage(msg, this.defaultMQProducer);

            if (!msg.getTopic().equals(mq.getTopic())) {
                throw new MQClientException("message's topic not equal mq's topic", null);
            }

            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeout < costTime) {
                throw new RemotingTooMuchRequestException("call timeout");
            }

            this.sendKernelImpl(msg, mq, CommunicationMode.SYNC, timeout)
                    .onFailure(promise::fail).onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }
        return promise.future();
    }

    /**
     * KERNEL ONEWAY -------------------------------------------------------
     */
    public Future<Void> sendOneway(Message msg, MessageQueue mq) {
        Promise<Void> promise = Promise.promise();
        try {
            this.makeSureStateOK();
            Validators.checkMessage(msg, this.defaultMQProducer);
            Future<SendResult> sendResultFuture = this.sendKernelImpl(msg, mq, CommunicationMode.ONEWAY,
                    this.defaultMQProducer.getSendMsgTimeout());
            sendResultFuture.onFailure(promise::fail);
            sendResultFuture.onSuccess(r -> promise.complete());
        } catch (Exception e) {
            promise.fail(e);
        }
        return promise.future();
    }

    /**
     * SELECT SYNC -------------------------------------------------------
     */
    public Future<SendResult> send(Message msg, MessageQueueSelector selector, Object arg) {
        return send(msg, selector, arg, this.defaultMQProducer.getSendMsgTimeout());
    }

    public Future<SendResult> send(Message msg, MessageQueueSelector selector, Object arg, long timeout) {
        Promise<SendResult> promise = Promise.promise();
        try {
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.SYNC, timeout)
                    .onFailure(promise::fail).onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }

        return promise.future();
    }

    private Future<SendResult> sendSelectImpl(
            Message msg,
            MessageQueueSelector selector,
            Object arg,
            final CommunicationMode communicationMode, final long timeout) throws MQClientException {
        long beginStartTime = System.currentTimeMillis();
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        Promise<SendResult> promise = Promise.promise();
        Future<TopicPublishInfo> topicPublishInfoFuture = this.tryToFindTopicPublishInfo(msg.getTopic());
        topicPublishInfoFuture.onFailure(promise::fail);
        topicPublishInfoFuture.onSuccess(topicPublishInfo -> {
            try {
                if (topicPublishInfo != null && topicPublishInfo.ok()) {
                    MessageQueue mq;
                    try {
                        List<MessageQueue> messageQueueList = mQClientFactory.getMQAdminImpl()
                                .parsePublishMessageQueues(topicPublishInfo.getMessageQueueList());
                        Message userMessage = MessageAccessor.cloneMessage(msg);
                        String userTopic = NamespaceUtil.withoutNamespace(userMessage.getTopic(),
                                mQClientFactory.getClientConfig().getNamespace());
                        userMessage.setTopic(userTopic);

                        mq = mQClientFactory.getClientConfig()
                                .queueWithNamespace(selector.select(messageQueueList, userMessage, arg));
                    } catch (Throwable e) {
                        throw new MQClientException("select message queue throwed exception.", e);
                    }

                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeout < costTime) {
                        throw new RemotingTooMuchRequestException("sendSelectImpl call timeout");
                    }
                    if (mq != null) {
                        this.sendKernelImpl(msg, mq, communicationMode, timeout - costTime)
                                .onFailure(promise::fail).onSuccess(promise::complete);
                    } else {
                        throw new MQClientException("select message queue return null.", null);
                    }
                } else {
                    validateNameServerSetting();
                    throw new MQClientException("No route info for this topic, " + msg.getTopic(), null);
                }
            } catch (Exception e) {
                promise.fail(e);
            }
        });

        return promise.future();
    }

    /**
     * SELECT ONEWAY -------------------------------------------------------
     */
    public Future<Void> sendOneway(Message msg, MessageQueueSelector selector, Object arg) {
        Promise<Void> promise = Promise.promise();
        try {
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ONEWAY, this.defaultMQProducer.getSendMsgTimeout());
        } catch (Exception e) {
            promise.fail(new MQClientException("unknown exception", e));
        }
        return promise.future();
    }

    public Future<TransactionSendResult> sendMessageInTransaction(final Message msg,
            final LocalTransactionExecuter localTransactionExecuter, final Object arg)
            throws MQClientException {

        Promise<TransactionSendResult> promise = Promise.promise();
        TransactionListener transactionListener = getCheckListener();
        if (null == localTransactionExecuter && null == transactionListener) {
            promise.fail(new MQClientException("tranExecutor is null", null));
            return promise.future();
        }

        // ignore DelayTimeLevel parameter
        if (msg.getDelayTimeLevel() != 0) {
            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        }

        Validators.checkMessage(msg, this.defaultMQProducer);

        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, this.defaultMQProducer.getProducerGroup());
        Future<SendResult> sendFuture = this.send(msg);
        sendFuture.onFailure(e -> promise.fail(new MQClientException("send message Exception", e)));
        sendFuture.onSuccess(sendResult -> {
            Future<LocalTransactionState> stateFuture;
            switch (sendResult.getSendStatus()) {
                case SEND_OK: {
                    if (sendResult.getTransactionId() != null) {
                        msg.putUserProperty("__transactionId__", sendResult.getTransactionId());
                    }
                    String transactionId = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                    if (null != transactionId && !"".equals(transactionId)) {
                        msg.setTransactionId(transactionId);
                    }
                    if (null != localTransactionExecuter) {
                        stateFuture = localTransactionExecuter.executeLocalTransactionBranch(msg, arg);
                    } else {
                        log.debug("Used new transaction API");
                        stateFuture = transactionListener.executeLocalTransaction(msg, arg);
                    }

                }
                    break;
                case FLUSH_DISK_TIMEOUT:
                case FLUSH_SLAVE_TIMEOUT:
                case SLAVE_NOT_AVAILABLE:
                    stateFuture = Future.succeededFuture(LocalTransactionState.ROLLBACK_MESSAGE);
                    break;
                default:
                    stateFuture = Future.succeededFuture(LocalTransactionState.UNKNOW);
                    break;
            }

            stateFuture.onComplete(ar -> {
                if (ar.failed()) {
                    log.info("executeLocalTransactionBranch exception", ar.cause());
                    log.info(msg.toString());
                }
                LocalTransactionState localTransactionState = ar.result();
                if (null == ar.result()) {
                    localTransactionState = LocalTransactionState.UNKNOW;
                }

                if (ar.result() != LocalTransactionState.COMMIT_MESSAGE) {
                    log.info("executeLocalTransactionBranch return {}", ar.result());
                    log.info(msg.toString());
                }
                LocalTransactionState finalLocalTransactionState = localTransactionState;
                this.endTransaction(sendResult, localTransactionState, ar.cause()).onComplete(ar1 -> {
                    if (ar1.failed()) {
                        log.warn("local transaction execute " + finalLocalTransactionState
                                + ", but end broker transaction failed", ar1.cause());
                    }
                    TransactionSendResult transactionSendResult = new TransactionSendResult();
                    transactionSendResult.setSendStatus(sendResult.getSendStatus());
                    transactionSendResult.setMessageQueue(sendResult.getMessageQueue());
                    transactionSendResult.setMsgId(sendResult.getMsgId());
                    transactionSendResult.setQueueOffset(sendResult.getQueueOffset());
                    transactionSendResult.setTransactionId(sendResult.getTransactionId());
                    transactionSendResult.setLocalTransactionState(finalLocalTransactionState);
                    promise.complete(transactionSendResult);
                });
            });
        });

        return promise.future();
    }

    /**
     * DEFAULT SYNC -------------------------------------------------------
     */
    public Future<SendResult> send(Message msg) {
        return send(msg, this.defaultMQProducer.getSendMsgTimeout());
    }

    public Future<Void> endTransaction(
            final SendResult sendResult,
            final LocalTransactionState localTransactionState,
            final Throwable localException) {
        final MessageId id;
        Promise<Void> promise = Promise.promise();
        try {
            if (sendResult.getOffsetMsgId() != null) {
                id = MessageDecoder.decodeMessageId(sendResult.getOffsetMsgId());
            } else {
                id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
            }
            String transactionId = sendResult.getTransactionId();
            final String brokerAddr = this.mQClientFactory
                    .findBrokerAddressInPublish(sendResult.getMessageQueue().getBrokerName());
            EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
            requestHeader.setTransactionId(transactionId);
            requestHeader.setCommitLogOffset(id.getOffset());
            switch (localTransactionState) {
                case COMMIT_MESSAGE:
                    requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                    break;
                case ROLLBACK_MESSAGE:
                    requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                    break;
                case UNKNOW:
                    requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                    break;
                default:
                    break;
            }

            requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
            requestHeader.setTranStateTableOffset(sendResult.getQueueOffset());
            requestHeader.setMsgId(sendResult.getMsgId());
            String remark = localException != null ? ("executeLocalTransactionBranch exception: " + localException) : null;
            this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, requestHeader, remark,
                    this.defaultMQProducer.getSendMsgTimeout()).onFailure(promise::fail).onSuccess(promise::complete);
        } catch (Exception e) {
            promise.fail(e);
        }

        return promise.future();
    }

    public Future<SendResult> send(Message msg, long timeout) {

        Promise<SendResult> promise = Promise.promise();
        Future<TopicPublishInfo> topicPublishInfoFuture = this.tryToFindTopicPublishInfo(msg.getTopic());
        topicPublishInfoFuture.onFailure(promise::fail);
        topicPublishInfoFuture.onSuccess(topicPublishInfo -> {
            try {
                this.makeSureStateOK();
                Validators.checkMessage(msg, this.defaultMQProducer);
                if (topicPublishInfo != null && topicPublishInfo.ok()) {
                    this.sendDefaultImpl(msg, CommunicationMode.SYNC, topicPublishInfo,
                            timeout, 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed(), promise, null);
                } else {
                    validateNameServerSetting();
                    promise.fail(new MQClientException(
                            "No route info of this topic: " + msg.getTopic() + FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO),
                            null).setResponseCode(ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION));
                }
            } catch (Exception e) {
                promise.fail(e);
            }

        });
        return promise.future();
    }

    public Future<Message> request(Message msg, long timeout) {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        long cost = System.currentTimeMillis() - beginTimestamp;
        Promise<Message> promise = Promise.promise();
        Future<TopicPublishInfo> topicPublishInfoFuture = this.tryToFindTopicPublishInfo(msg.getTopic());
        topicPublishInfoFuture.onFailure(promise::fail);
        topicPublishInfoFuture.onSuccess(topicPublishInfo -> {
            RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, promise);
            RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);
            MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, null);
            Future<SendResult> sendResultFuture = sendKernelImpl(msg, mqSelected, CommunicationMode.ASYNC, timeout - cost);
            sendResultFuture.onFailure(promise::fail);
            sendResultFuture.onSuccess(v -> {
                requestResponseFuture.requestOk();
                vertx.setTimer(timeout - cost, id -> {
                    RequestResponseFuture timeOutFuture = RequestFutureTable.getRequestFutureTable().get(correlationId);
                    if (!Objects.isNull(timeOutFuture) && !timeOutFuture.getPromise().future().isComplete()) {
                        RequestFutureTable.getRequestFutureTable().remove(correlationId);
                        if (timeOutFuture.isRequestOk()) {
                            timeOutFuture.getPromise()
                                    .fail(new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
                                            "send request message to <" + msg.getTopic()
                                                    + "> OK, but wait reply message timeout, " + timeout + " ms."));
                        } else {
                            timeOutFuture.getPromise().fail(new MQClientException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
                                    "send request message to <" + msg.getTopic() + "> fail"));
                        }
                    }
                });
            });
        });
        return promise.future();
    }

    public Future<Message> request(final Message msg, final MessageQueueSelector selector, final Object arg,
            final long timeout) {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        Promise<Message> promise = Promise.promise();
        RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, promise);
        RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        try {
            Future<SendResult> sendResultFuture = this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC,
                    timeout - cost);
            sendResultFuture.onFailure(promise::fail);
            sendResultFuture.onSuccess(v -> {
                requestResponseFuture.requestOk();
                vertx.setTimer(timeout - cost, id -> {
                    RequestResponseFuture timeOutFuture = RequestFutureTable.getRequestFutureTable().get(correlationId);
                    if (!Objects.isNull(timeOutFuture) && !timeOutFuture.getPromise().future().isComplete()) {
                        RequestFutureTable.getRequestFutureTable().remove(correlationId);
                        if (timeOutFuture.isRequestOk()) {
                            timeOutFuture.getPromise()
                                    .fail(new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
                                            "send request message to <" + msg.getTopic()
                                                    + "> OK, but wait reply message timeout, " + timeout + " ms."));
                        } else {
                            timeOutFuture.getPromise().fail(new MQClientException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
                                    "send request message to <" + msg.getTopic() + "> fail"));
                        }
                    }
                });
            });

        } catch (Exception e) {
            promise.fail(e);
        }
        return promise.future();
    }

    private void prepareSendRequest(final Message msg, long timeout) {
        String correlationId = CorrelationIdUtil.createCorrelationId();
        String requestClientId = this.getmQClientFactory().getClientId();
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, requestClientId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_TTL, String.valueOf(timeout));

        boolean hasRouteData = this.getmQClientFactory().getTopicRouteTable().containsKey(msg.getTopic());
        if (!hasRouteData) {
            long beginTimestamp = System.currentTimeMillis();
            this.tryToFindTopicPublishInfo(msg.getTopic());
            this.getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
            long cost = System.currentTimeMillis() - beginTimestamp;
            if (cost > 500) {
                log.warn("prepare send request for <{}> cost {} ms", msg.getTopic(), cost);
            }
        }
    }

    public ConcurrentMap<String, TopicPublishInfo> getTopicPublishInfoTable() {
        return topicPublishInfoTable;
    }

    public int getZipCompressLevel() {
        return zipCompressLevel;
    }

    public void setZipCompressLevel(int zipCompressLevel) {
        this.zipCompressLevel = zipCompressLevel;
    }

    public ServiceState getServiceState() {
        return serviceState.get();
    }

    public void setServiceState(ServiceState serviceState) {
        this.serviceState = new AtomicReference<>(serviceState);
    }

    public long[] getNotAvailableDuration() {
        return this.mqFaultStrategy.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.mqFaultStrategy.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.mqFaultStrategy.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.mqFaultStrategy.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.mqFaultStrategy.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.mqFaultStrategy.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }
}
