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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.CheckClientRequestBody;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.LockBatchResponseBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.common.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.filtersrv.RegisterMessageFilterClassRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.rocketmq.client.QueryResult;
import io.vertx.rocketmq.client.common.RocketmqOptions;
import io.vertx.rocketmq.client.common.TopAddress;
import io.vertx.rocketmq.client.connection.RocketmqConnection;
import io.vertx.rocketmq.client.connection.RocketmqConnectionManager;
import io.vertx.rocketmq.client.consumer.PullResult;
import io.vertx.rocketmq.client.consumer.PullStatus;
import io.vertx.rocketmq.client.exception.MQBrokerException;
import io.vertx.rocketmq.client.exception.MQClientException;
import io.vertx.rocketmq.client.impl.factory.MQClientInstance;
import io.vertx.rocketmq.client.log.ClientLogger;
import io.vertx.rocketmq.client.producer.SendResult;
import io.vertx.rocketmq.client.producer.SendStatus;
import io.vertx.rocketmq.client.producer.consts.CommunicationMode;

public class MQClientAPIImpl {

    private final static InternalLogger log = ClientLogger.getLog();
    private static final boolean sendSmartMsg = Boolean
            .parseBoolean(System.getProperty("org.apache.rocketmq.client.sendSmartMsg", "true"));

    static {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
    }

    private final TopAddress topAddress;
    private String nameSrvAddr = null;
    private final VertxInternal vertx;
    private final RocketmqConnectionManager connectionManager;
    private final Semaphore semaphoreOneway;
    private final RocketmqOptions options;

    public MQClientAPIImpl(VertxInternal vertx, RocketmqOptions options, MQClientInstance mqClientFactory) {
        this.options = options;
        topAddress = new TopAddress(MixAll.getWSAddr(), vertx);
        this.vertx = vertx;
        this.connectionManager = new RocketmqConnectionManager(vertx, options, mqClientFactory);
        this.semaphoreOneway = new Semaphore(options.getPermitsOneway(), true);
    }

    public Future<Void> shutdown() {
        return this.connectionManager.shutdown();
    }

    public List<String> getNameServerAddressList() {
        return connectionManager.getNameServerAddressList();
    }

    public Future<String> fetchNameServerAddr() {
        PromiseInternal<String> promise = vertx.promise();
        Future<String> fetchNSAddr = topAddress.fetchNSAddr(3000);
        fetchNSAddr.onFailure(promise::fail).onSuccess(addrs -> {
            if (addrs != null) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    promise.complete(nameSrvAddr);
                }
            }
        });

        return promise.future();
    }

    public void updateNameServerAddressList(final String addrs) {
        String[] addrArray = addrs.split(";");
        List<String> list = Arrays.asList(addrArray);
        connectionManager.updateNameServerAddressList(list);
    }

    public Future<Void> createTopic(final String addr, final String defaultTopic, final TopicConfig topicConfig,
            final long timeoutMillis) {
        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
        requestHeader.setTopic(topicConfig.getTopicName());
        requestHeader.setDefaultTopic(defaultTopic);
        requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
        requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
        requestHeader.setPerm(topicConfig.getPerm());
        requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());
        requestHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
        requestHeader.setOrder(topicConfig.isOrder());
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);

        PromiseInternal<Void> promise = vertx.promise();
        Future<RocketmqConnection> connectionFuture = connectionManager
                .getConnection(MixAll.brokerVIPChannel(this.options.isVipChannelEnabled(), addr));
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                if (ResponseCode.SUCCESS == response.getCode()) {
                    promise.complete();
                } else {
                    promise.fail(new MQClientException(response.getCode(), response.getRemark()));
                }
            });
        });

        return promise.future();
    }

    public Future<SendResult> sendMessage(
            final String addr,
            final String brokerName,
            final Message msg,
            final SendMessageRequestHeader requestHeader,
            final long timeoutMillis,
            final CommunicationMode communicationMode) {
        long beginStartTime = System.currentTimeMillis();
        RemotingCommand request;
        String msgType = msg.getProperty(MessageConst.PROPERTY_MESSAGE_TYPE);
        boolean isReply = msgType != null && msgType.equals(MixAll.REPLY_MESSAGE_FLAG);
        if (isReply) {
            if (sendSmartMsg) {
                SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2
                        .createSendMessageRequestHeaderV2(requestHeader);
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_REPLY_MESSAGE_V2, requestHeaderV2);
            } else {
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_REPLY_MESSAGE, requestHeader);
            }
        } else {
            if (sendSmartMsg || msg instanceof MessageBatch) {
                SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2
                        .createSendMessageRequestHeaderV2(requestHeader);
                request = RemotingCommand.createRequestCommand(
                        msg instanceof MessageBatch ? RequestCode.SEND_BATCH_MESSAGE : RequestCode.SEND_MESSAGE_V2,
                        requestHeaderV2);
            } else {
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
            }
        }
        request.setBody(msg.getBody());
        Promise<SendResult> promise = Promise.promise();
        switch (communicationMode) {
            case ONEWAY:
                this.invokeOneway(addr, request);
                promise.complete();
                break;
            case SYNC:
                long costTimeSync = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTimeSync) {
                    promise.fail(new RemotingTooMuchRequestException("sendMessage call timeout"));
                } else {
                    Future<SendResult> resultFuture = this.sendMessageSync(addr, brokerName, msg, timeoutMillis - costTimeSync,
                            request);
                    resultFuture.onFailure(promise::fail);
                    resultFuture.onSuccess(promise::complete);
                }
                break;
            case ASYNC:
                Future<Void> voidFuture = sendMessageAsync(addr, request);
                voidFuture.onFailure(promise::fail);
                voidFuture.onSuccess(v -> promise.complete());
                break;
            default:
                assert false;
                break;
        }

        return promise.future();
    }

    private Future<Void> invokeOneway(String addr, RemotingCommand request) {
        Promise<Void> promise = Promise.promise();
        Future<RocketmqConnection> connectionFuture = connectionManager.getConnection(addr);
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> send = conn.send(request, CommunicationMode.ASYNC);
            send.onFailure(promise::fail);
            send.onSuccess(v -> promise.complete());
        });
        return promise.future();
    }

    private Future<SendResult> sendMessageSync(
            final String addr,
            final String brokerName,
            final Message msg,
            final long timeoutMillis,
            final RemotingCommand request) {
        Promise<SendResult> promise = Promise.promise();
        Future<RocketmqConnection> connFuture = connectionManager.getConnection(addr);
        connFuture.onFailure(promise::fail);
        connFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                try {
                    promise.complete(this.processSendResponse(brokerName, msg, response));
                } catch (Exception e) {
                    promise.fail(e);
                }
            });
        });
        return promise.future();
    }

    private Future<Void> sendMessageAsync(final String addr, RemotingCommand request) {
        Promise<Void> promise = Promise.promise();
        Future<RocketmqConnection> connFuture = connectionManager.getConnection(addr);
        connFuture.onFailure(promise::fail);
        connFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.ASYNC, null);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> promise.complete());
        });
        return promise.future();
    }

    private SendResult processSendResponse(
            final String brokerName,
            final Message msg,
            final RemotingCommand response) throws MQBrokerException, RemotingCommandException {
        SendStatus sendStatus;
        switch (response.getCode()) {
            case ResponseCode.FLUSH_DISK_TIMEOUT: {
                sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
                break;
            }
            case ResponseCode.FLUSH_SLAVE_TIMEOUT: {
                sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
                break;
            }
            case ResponseCode.SLAVE_NOT_AVAILABLE: {
                sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
                break;
            }
            case ResponseCode.SUCCESS: {
                sendStatus = SendStatus.SEND_OK;
                break;
            }
            default: {
                throw new MQBrokerException(response.getCode(), response.getRemark());
            }
        }

        SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response
                .decodeCommandCustomHeader(SendMessageResponseHeader.class);

        //If namespace not null , reset Topic without namespace.
        String topic = msg.getTopic();
        if (StringUtils.isNotEmpty(this.options.getNamespace())) {
            topic = NamespaceUtil.withoutNamespace(topic, this.options.getNamespace());
        }

        MessageQueue messageQueue = new MessageQueue(topic, brokerName, responseHeader.getQueueId());

        String uniqMsgId = MessageClientIDSetter.getUniqID(msg);
        if (msg instanceof MessageBatch) {
            StringBuilder sb = new StringBuilder();
            for (Message message : (MessageBatch) msg) {
                sb.append(sb.length() == 0 ? "" : ",").append(MessageClientIDSetter.getUniqID(message));
            }
            uniqMsgId = sb.toString();
        }
        SendResult sendResult = new SendResult(sendStatus,
                uniqMsgId,
                responseHeader.getMsgId(), messageQueue, responseHeader.getQueueOffset());
        sendResult.setTransactionId(responseHeader.getTransactionId());
        String regionId = response.getExtFields().get(MessageConst.PROPERTY_MSG_REGION);
        String traceOn = response.getExtFields().get(MessageConst.PROPERTY_TRACE_SWITCH);
        if (regionId == null || regionId.isEmpty()) {
            regionId = MixAll.DEFAULT_TRACE_REGION_ID;
        }
        sendResult.setTraceOn(traceOn == null || !traceOn.equals("false"));
        sendResult.setRegionId(regionId);
        return sendResult;
    }

    public Future<PullResult> pullMessage(
            final String addr,
            final PullMessageRequestHeader requestHeader,
            final long timeoutMillis) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);
        Promise<PullResult> promise = Promise.promise();
        Future<RocketmqConnection> connectionFuture = connectionManager.getConnection(addr);
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                try {
                    promise.complete(this.processPullResponse(response));
                } catch (Exception e) {
                    promise.fail(e);
                }
            });
        });
        return promise.future();
    }

    private PullResult processPullResponse(
            final RemotingCommand response) throws MQBrokerException, RemotingCommandException {
        PullStatus pullStatus;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                pullStatus = PullStatus.FOUND;
                break;
            case ResponseCode.PULL_NOT_FOUND:
                pullStatus = PullStatus.NO_NEW_MSG;
                break;
            case ResponseCode.PULL_RETRY_IMMEDIATELY:
                pullStatus = PullStatus.NO_MATCHED_MSG;
                break;
            case ResponseCode.PULL_OFFSET_MOVED:
                pullStatus = PullStatus.OFFSET_ILLEGAL;
                break;

            default:
                throw new MQBrokerException(response.getCode(), response.getRemark());
        }

        PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response
                .decodeCommandCustomHeader(PullMessageResponseHeader.class);

        return new PullResultExt(pullStatus, responseHeader.getNextBeginOffset(), responseHeader.getMinOffset(),
                responseHeader.getMaxOffset(), null, responseHeader.getSuggestWhichBrokerId(), response.getBody());
    }

    public Future<MessageExt> viewMessage(final String addr, final long phyoffset, final long timeoutMillis) {
        ViewMessageRequestHeader requestHeader = new ViewMessageRequestHeader();
        requestHeader.setOffset(phyoffset);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, requestHeader);

        Promise<MessageExt> promise = Promise.promise();
        Future<RocketmqConnection> connectionFuture = connectionManager
                .getConnection(MixAll.brokerVIPChannel(this.options.isVipChannelEnabled(), addr));
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                if (ResponseCode.SUCCESS == response.getCode()) {
                    ByteBuffer byteBuffer = ByteBuffer.wrap(response.getBody());
                    MessageExt messageExt = MessageDecoder.clientDecode(byteBuffer, true);
                    //If namespace not null , reset Topic without namespace.
                    if (StringUtils.isNotEmpty(this.options.getNamespace())) {
                        messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.options.getNamespace()));
                    }
                    promise.complete(messageExt);
                } else {
                    promise.fail(new MQBrokerException(response.getCode(), response.getRemark()));
                }
            });
        });

        return promise.future();
    }

    public Future<Long> searchOffset(final String addr, final String topic, final int queueId, final long timestamp,
            final long timeoutMillis) {
        Promise<Long> promise = Promise.promise();
        SearchOffsetRequestHeader requestHeader = new SearchOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setTimestamp(timestamp);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);
        Future<RocketmqConnection> connectionFuture = connectionManager
                .getConnection(MixAll.brokerVIPChannel(this.options.isVipChannelEnabled(), addr));
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                if (ResponseCode.SUCCESS == response.getCode()) {
                    try {
                        SearchOffsetResponseHeader responseHeader = (SearchOffsetResponseHeader) response
                                .decodeCommandCustomHeader(SearchOffsetResponseHeader.class);
                        promise.complete(responseHeader.getOffset());
                    } catch (RemotingCommandException e) {
                        promise.fail(e);
                    }
                } else {
                    promise.fail(new MQBrokerException(response.getCode(), response.getRemark()));
                }
            });
        });

        return promise.future();
    }

    public Future<Long> getMaxOffset(final String addr, final String topic, final int queueId, final long timeoutMillis) {
        Promise<Long> promise = Promise.promise();

        GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, requestHeader);
        Future<RocketmqConnection> connectionFuture = connectionManager
                .getConnection(MixAll.brokerVIPChannel(this.options.isVipChannelEnabled(), addr));
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                if (ResponseCode.SUCCESS == response.getCode()) {
                    try {
                        GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response
                                .decodeCommandCustomHeader(GetMaxOffsetResponseHeader.class);
                        promise.complete(responseHeader.getOffset());
                    } catch (RemotingCommandException e) {
                        promise.fail(e);
                    }
                } else {
                    promise.fail(new MQBrokerException(response.getCode(), response.getRemark()));
                }
            });
        });

        return promise.future();
    }

    public Future<List<String>> getConsumerIdListByGroup(
            final String addr,
            final String consumerGroup,
            final long timeoutMillis) {
        GetConsumerListByGroupRequestHeader requestHeader = new GetConsumerListByGroupRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);

        PromiseInternal<List<String>> promise = vertx.promise();
        Future<RocketmqConnection> connectionFuture = connectionManager
                .getConnection(MixAll.brokerVIPChannel(this.options.isVipChannelEnabled(), addr));
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                if (ResponseCode.SUCCESS == response.getCode() && response.getBody() != null) {
                    GetConsumerListByGroupResponseBody body = GetConsumerListByGroupResponseBody.decode(response.getBody(),
                            GetConsumerListByGroupResponseBody.class);
                    promise.complete(body.getConsumerIdList());
                } else {
                    promise.fail(new MQBrokerException(response.getCode(), response.getRemark()));
                }
            });
        });
        return promise.future();
    }

    public Future<Long> getMinOffset(final String addr, final String topic, final int queueId, final long timeoutMillis) {
        GetMinOffsetRequestHeader requestHeader = new GetMinOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, requestHeader);
        Promise<Long> promise = Promise.promise();
        Future<RocketmqConnection> connectionFuture = connectionManager
                .getConnection(MixAll.brokerVIPChannel(this.options.isVipChannelEnabled(), addr));
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                if (ResponseCode.SUCCESS == response.getCode()) {
                    try {
                        GetMinOffsetResponseHeader responseHeader = (GetMinOffsetResponseHeader) response
                                .decodeCommandCustomHeader(GetMinOffsetResponseHeader.class);
                        promise.complete(responseHeader.getOffset());
                    } catch (RemotingCommandException e) {
                        promise.fail(e);
                    }
                } else {
                    promise.fail(new MQBrokerException(response.getCode(), response.getRemark()));
                }
            });
        });

        return promise.future();
    }

    public Future<Long> getEarliestMsgStoretime(final String addr, final String topic, final int queueId,
            final long timeoutMillis) {
        GetEarliestMsgStoretimeRequestHeader requestHeader = new GetEarliestMsgStoretimeRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_EARLIEST_MSG_STORETIME, requestHeader);
        Promise<Long> promise = Promise.promise();
        Future<RocketmqConnection> connectionFuture = connectionManager
                .getConnection(MixAll.brokerVIPChannel(this.options.isVipChannelEnabled(), addr));
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                if (ResponseCode.SUCCESS == response.getCode()) {
                    try {
                        GetEarliestMsgStoretimeResponseHeader responseHeader = (GetEarliestMsgStoretimeResponseHeader) response
                                .decodeCommandCustomHeader(GetEarliestMsgStoretimeResponseHeader.class);
                        promise.complete(responseHeader.getTimestamp());
                    } catch (RemotingCommandException e) {
                        promise.fail(e);
                    }
                } else {
                    promise.fail(new MQBrokerException(response.getCode(), response.getRemark()));
                }
            });
        });
        return promise.future();
    }

    public Future<Long> queryConsumerOffset(
            final String addr,
            final QueryConsumerOffsetRequestHeader requestHeader,
            final long timeoutMillis) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader);
        Promise<Long> promise = Promise.promise();
        Future<RocketmqConnection> connectionFuture = connectionManager
                .getConnection(MixAll.brokerVIPChannel(this.options.isVipChannelEnabled(), addr));
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                if (ResponseCode.SUCCESS == response.getCode()) {
                    try {
                        QueryConsumerOffsetResponseHeader responseHeader = (QueryConsumerOffsetResponseHeader) response
                                .decodeCommandCustomHeader(QueryConsumerOffsetResponseHeader.class);
                        promise.complete(responseHeader.getOffset());
                    } catch (RemotingCommandException e) {
                        promise.fail(e);
                    }
                } else {
                    promise.fail(new MQBrokerException(response.getCode(), response.getRemark()));
                }
            });
        });

        return promise.future();
    }

    public Future<Void> updateConsumerOffset(
            final String addr,
            final UpdateConsumerOffsetRequestHeader requestHeader,
            final long timeoutMillis) {
        Promise<Void> promise = Promise.promise();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);
        Future<RocketmqConnection> connectionFuture = connectionManager
                .getConnection(MixAll.brokerVIPChannel(this.options.isVipChannelEnabled(), addr));
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> sendFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            sendFuture.onFailure(promise::fail);
            sendFuture.onSuccess(response -> {
                if (ResponseCode.SUCCESS == response.getCode()) {
                    promise.complete();
                } else {
                    promise.fail(new MQBrokerException(response.getCode(), response.getRemark()));
                }
            });
        });

        return promise.future();
    }

    public Future<Void> updateConsumerOffsetOneway(
            final String addr,
            final UpdateConsumerOffsetRequestHeader requestHeader) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);
        Promise<Void> promise = Promise.promise();
        Future<RocketmqConnection> connectionFuture = connectionManager
                .getConnection(MixAll.brokerVIPChannel(this.options.isVipChannelEnabled(), addr));
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.ONEWAY);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> promise.complete());
        });

        return promise.future();
    }

    public Future<Integer> sendHearbeat(
            final String addr,
            final HeartbeatData heartbeatData,
            final long timeoutMillis) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setLanguage(options.getLanguage());
        request.setBody(heartbeatData.encode());

        PromiseInternal<Integer> promise = vertx.promise();
        Future<RocketmqConnection> connectionFuture = connectionManager.getConnection(addr);
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                if (ResponseCode.SUCCESS == response.getCode()) {
                    promise.complete(response.getVersion());
                } else {
                    promise.fail(new MQBrokerException(response.getCode(), response.getRemark()));
                }
            });
        });

        return promise.future();
    }

    public Future<Void> unregisterClient(
            final String addr,
            final String clientID,
            final String producerGroup,
            final String consumerGroup,
            final long timeoutMillis) {
        final UnregisterClientRequestHeader requestHeader = new UnregisterClientRequestHeader();
        requestHeader.setClientID(clientID);
        requestHeader.setProducerGroup(producerGroup);
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, requestHeader);

        Promise<Void> promise = Promise.promise();
        Future<RocketmqConnection> connectionFuture = connectionManager.getConnection(addr);
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                if (response.getCode() == ResponseCode.SUCCESS) {
                    promise.complete();
                } else {
                    promise.fail(new MQBrokerException(response.getCode(), response.getRemark()));
                }
            });
        });
        return promise.future();
    }

    public Future<Void> endTransactionOneway(
            final String addr,
            final EndTransactionRequestHeader requestHeader,
            final String remark,
            final long timeoutMillis) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader);
        request.setRemark(remark);
        return invokeOnewayImpl(addr, request, timeoutMillis);
    }

    private Future<Void> invokeOnewayImpl(final String addr, final RemotingCommand request, final long timeoutMillis) {
        request.markOnewayRPC();
        return vertx.executeBlocking(promise -> {
            boolean acquired;
            try {
                acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                promise.fail(e);
                return;
            }
            if (acquired) {
                final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
                Future<RocketmqConnection> connectionFuture = connectionManager.getConnection(addr);
                connectionFuture.onFailure(promise::fail);
                connectionFuture.onSuccess(conn -> {
                    Future<RemotingCommand> sendFuture = conn.send(request, CommunicationMode.ASYNC);
                    sendFuture.onComplete(ar -> {
                        once.release();
                        if (ar.failed()) {
                            log.warn("send a request command to channel <" + addr + "> failed.");
                        }
                        promise.complete();
                    });
                });
            } else {
                if (timeoutMillis <= 0) {
                    promise.fail(new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast"));
                } else {
                    String info = String.format(
                            "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                            timeoutMillis,
                            this.semaphoreOneway.getQueueLength(),
                            this.semaphoreOneway.availablePermits());
                    log.warn(info);
                    promise.fail(new RemotingTimeoutException(info));
                }
            }
        });

    }

    public Future<QueryResult> queryMessage(
            final String addr,
            final QueryMessageRequestHeader requestHeader,
            final long timeoutMillis,
            final Boolean isUnqiueKey) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, requestHeader);
        request.addExtField(MixAll.UNIQUE_MSG_QUERY_FLAG, isUnqiueKey.toString());
        Promise<QueryResult> promise = Promise.promise();
        Future<RocketmqConnection> connectionFuture = connectionManager
                .getConnection(MixAll.brokerVIPChannel(this.options.isVipChannelEnabled(), addr));
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                if (ResponseCode.SUCCESS == response.getCode()) {
                    try {
                        QueryMessageResponseHeader responseHeader = (QueryMessageResponseHeader) response
                                .decodeCommandCustomHeader(QueryMessageResponseHeader.class);
                        List<MessageExt> wrappers = MessageDecoder.decodes(ByteBuffer.wrap(response.getBody()), true);
                        promise.complete(new QueryResult(responseHeader.getIndexLastUpdateTimestamp(), wrappers));
                    } catch (RemotingCommandException e) {
                        promise.fail(e);
                    }
                } else {
                    promise.fail(new MQBrokerException(response.getCode(), response.getRemark()));
                }
            });
        });
        return promise.future();
    }

    public Future<Void> consumerSendMessageBack(
            final String addr,
            final MessageExt msg,
            final String consumerGroup,
            final int delayLevel,
            final long timeoutMillis,
            final int maxConsumeRetryTimes) {
        ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);

        requestHeader.setGroup(consumerGroup);
        requestHeader.setOriginTopic(msg.getTopic());
        requestHeader.setOffset(msg.getCommitLogOffset());
        requestHeader.setDelayLevel(delayLevel);
        requestHeader.setOriginMsgId(msg.getMsgId());
        requestHeader.setMaxReconsumeTimes(maxConsumeRetryTimes);

        Promise<Void> promise = Promise.promise();
        Future<RocketmqConnection> connectionFuture = connectionManager
                .getConnection(MixAll.brokerVIPChannel(this.options.isVipChannelEnabled(), addr));
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                if (ResponseCode.SUCCESS == response.getCode()) {
                    promise.complete();
                } else {
                    promise.fail(new MQBrokerException(response.getCode(), response.getRemark()));
                }
            });
        });
        return promise.future();
    }

    public Future<Set<MessageQueue>> lockBatchMQ(
            final String addr,
            final LockBatchRequestBody requestBody,
            final long timeoutMillis) {

        PromiseInternal<Set<MessageQueue>> promise = vertx.promise();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
        request.setBody(requestBody.encode());
        Future<RocketmqConnection> connectionFuture = connectionManager
                .getConnection(MixAll.brokerVIPChannel(this.options.isVipChannelEnabled(), addr));
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                if (ResponseCode.SUCCESS == response.getCode()) {
                    LockBatchResponseBody responseBody = LockBatchResponseBody.decode(response.getBody(),
                            LockBatchResponseBody.class);
                    promise.complete(responseBody.getLockOKMQSet());
                } else {
                    promise.fail(new MQBrokerException(response.getCode(), response.getRemark()));
                }
            });
        });

        return promise.future();
    }

    public Future<Void> unlockBatchMQ(
            final String addr,
            final UnlockBatchRequestBody requestBody,
            final long timeoutMillis,
            final boolean oneway) {

        PromiseInternal<Void> promise = vertx.promise();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, null);
        request.setBody(requestBody.encode());

        if (oneway) {
            Future<RocketmqConnection> connectionFuture = connectionManager.getConnection(addr);
            connectionFuture.onFailure(promise::fail);
            connectionFuture.onSuccess(conn -> {
                Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.ONEWAY);
                commandFuture.onFailure(promise::fail);
                commandFuture.onSuccess(response -> promise.complete());
            });
        } else {
            Future<RocketmqConnection> connectionFuture = connectionManager
                    .getConnection(MixAll.brokerVIPChannel(this.options.isVipChannelEnabled(), addr));
            connectionFuture.onFailure(promise::fail);
            connectionFuture.onSuccess(conn -> {
                Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
                commandFuture.onFailure(promise::fail);
                commandFuture.onSuccess(response -> {
                    if (ResponseCode.SUCCESS == response.getCode()) {
                        promise.complete();
                    } else {
                        promise.fail(new MQBrokerException(response.getCode(), response.getRemark()));
                    }
                });
            });
        }

        return promise.future();
    }

    public Future<TopicRouteData> getTopicRouteInfoFromNameServer(String topic, final long timeoutMillis) {
        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, requestHeader);

        PromiseInternal<TopicRouteData> promise = vertx.promise();
        Future<RocketmqConnection> connectionFuture = connectionManager.getConnection(null);
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                if (ResponseCode.SUCCESS == response.getCode() && !Objects.isNull(response.getBody())) {
                    promise.complete(TopicRouteData.decode(response.getBody(), TopicRouteData.class));
                } else {
                    promise.fail(new MQClientException(response.getCode(), response.getRemark()));
                }
            });
        });

        return promise.future();
    }

    public Future<Void> registerMessageFilterClass(final String addr,
            final String consumerGroup,
            final String topic,
            final String className,
            final int classCRC,
            final byte[] classBody,
            final long timeoutMillis) {
        RegisterMessageFilterClassRequestHeader requestHeader = new RegisterMessageFilterClassRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setClassName(className);
        requestHeader.setTopic(topic);
        requestHeader.setClassCRC(classCRC);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_MESSAGE_FILTER_CLASS,
                requestHeader);
        request.setBody(classBody);
        PromiseInternal<Void> promise = vertx.promise();
        Future<RocketmqConnection> connectionFuture = connectionManager.getConnection(addr);
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> sendFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            sendFuture.onFailure(promise::fail);
            sendFuture.onSuccess(response -> {
                if (ResponseCode.SUCCESS == response.getCode()) {
                    promise.complete();
                } else {
                    promise.fail(new MQBrokerException(response.getCode(), response.getRemark()));
                }
            });
        });
        return promise.future();
    }

    public Future<Void> checkClientInBroker(final String brokerAddr, final String consumerGroup,
            final String clientId, final SubscriptionData subscriptionData,
            final long timeoutMillis) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHECK_CLIENT_CONFIG, null);

        CheckClientRequestBody requestBody = new CheckClientRequestBody();
        requestBody.setClientId(clientId);
        requestBody.setGroup(consumerGroup);
        requestBody.setSubscriptionData(subscriptionData);

        request.setBody(requestBody.encode());

        Promise<Void> promise = Promise.promise();
        Future<RocketmqConnection> connectionFuture = connectionManager
                .getConnection(MixAll.brokerVIPChannel(this.options.isVipChannelEnabled(), brokerAddr));
        connectionFuture.onFailure(promise::fail);
        connectionFuture.onSuccess(conn -> {
            Future<RemotingCommand> commandFuture = conn.send(request, CommunicationMode.SYNC, timeoutMillis);
            commandFuture.onFailure(promise::fail);
            commandFuture.onSuccess(response -> {
                if (ResponseCode.SUCCESS == response.getCode()) {
                    promise.complete();
                } else {
                    promise.fail(new MQClientException(response.getCode(), response.getRemark()));
                }
            });
        });
        return promise.future();
    }
}
