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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import org.apache.rocketmq.common.protocol.header.ReplyMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import io.netty.channel.ChannelHandlerContext;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.rocketmq.client.impl.factory.MQClientInstance;
import io.vertx.rocketmq.client.impl.producer.MQProducerInner;
import io.vertx.rocketmq.client.log.ClientLogger;
import io.vertx.rocketmq.client.producer.RequestFutureTable;
import io.vertx.rocketmq.client.producer.RequestResponseFuture;

public class ClientRemotingProcessor {
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mqClientFactory;

    public ClientRemotingProcessor(final MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    public Future<RemotingCommand> processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            switch (request.getCode()) {
                case RequestCode.CHECK_TRANSACTION_STATE:
                    this.checkTransactionState(ctx, request);
                    return Future.succeededFuture();
                case RequestCode.NOTIFY_CONSUMER_IDS_CHANGED:
                    this.notifyConsumerIdsChanged(ctx, request);
                    return Future.succeededFuture();
                case RequestCode.RESET_CONSUMER_CLIENT_OFFSET:
                    return this.resetOffset(ctx, request);
                case RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT:
                    return this.getConsumeStatus(ctx, request);

                case RequestCode.GET_CONSUMER_RUNNING_INFO:
                    return this.getConsumerRunningInfo(ctx, request);

                case RequestCode.CONSUME_MESSAGE_DIRECTLY:
                    return this.consumeMessageDirectly(ctx, request);

                case RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT:
                    return this.receiveReplyMessage(ctx, request);
                default:
                    break;
            }
        } catch (Exception e) {
            return Future.failedFuture(e);
        }

        return Future.succeededFuture();
    }

    public RemotingCommand checkTransactionState(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final CheckTransactionStateRequestHeader requestHeader = (CheckTransactionStateRequestHeader) request
                .decodeCommandCustomHeader(CheckTransactionStateRequestHeader.class);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(request.getBody());
        final MessageExt messageExt = MessageDecoder.decode(byteBuffer);
        if (messageExt != null) {
            if (StringUtils.isNotEmpty(this.mqClientFactory.getClientConfig().getNamespace())) {
                messageExt.setTopic(NamespaceUtil
                        .withoutNamespace(messageExt.getTopic(), this.mqClientFactory.getClientConfig().getNamespace()));
            }
            String transactionId = messageExt.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
            if (null != transactionId && !"".equals(transactionId)) {
                messageExt.setTransactionId(transactionId);
            }
            final String group = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if (group != null) {
                MQProducerInner producer = this.mqClientFactory.selectProducer(group);
                if (producer != null) {
                    final String addr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    producer.checkTransactionState(addr, messageExt, requestHeader);
                } else {
                    log.debug("checkTransactionState, pick producer by group[{}] failed", group);
                }
            } else {
                log.warn("checkTransactionState, pick producer group failed");
            }
        } else {
            log.warn("checkTransactionState, decode message failed");
        }

        return null;
    }

    public RemotingCommand notifyConsumerIdsChanged(ChannelHandlerContext ctx, RemotingCommand request) {
        try {
            final NotifyConsumerIdsChangedRequestHeader requestHeader = (NotifyConsumerIdsChangedRequestHeader) request
                    .decodeCommandCustomHeader(NotifyConsumerIdsChangedRequestHeader.class);
            log.info("receive broker's notification[{}], the consumer group: {} changed, rebalance immediately",
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                    requestHeader.getConsumerGroup());
            this.mqClientFactory.rebalanceImmediately();
        } catch (Exception e) {
            log.error("notifyConsumerIdsChanged exception", RemotingHelper.exceptionSimpleDesc(e));
        }
        return null;
    }

    public Future<RemotingCommand> resetOffset(ChannelHandlerContext ctx, RemotingCommand request) {
        Promise<RemotingCommand> promise = Promise.promise();
        try {
            final ResetOffsetRequestHeader requestHeader = (ResetOffsetRequestHeader) request
                    .decodeCommandCustomHeader(ResetOffsetRequestHeader.class);
            log.info("invoke reset offset operation from broker. brokerAddr={}, topic={}, group={}, timestamp={}",
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(), requestHeader.getGroup(),
                    requestHeader.getTimestamp());
            Map<MessageQueue, Long> offsetTable = new HashMap<>();
            if (request.getBody() != null) {
                ResetOffsetBody body = ResetOffsetBody.decode(request.getBody(), ResetOffsetBody.class);
                offsetTable = body.getOffsetTable();
            }
            this.mqClientFactory.resetOffset(requestHeader.getTopic(), requestHeader.getGroup(), offsetTable)
                    .onComplete(v -> promise.complete());
        } catch (Exception e) {
            promise.fail(e);
        }
        return promise.future();
    }

    @Deprecated
    public Future<RemotingCommand> getConsumeStatus(ChannelHandlerContext ctx, RemotingCommand request) {
        Promise<RemotingCommand> promise = Promise.promise();
        try {
            final RemotingCommand response = RemotingCommand.createResponseCommand(null);
            final GetConsumerStatusRequestHeader requestHeader = (GetConsumerStatusRequestHeader) request
                    .decodeCommandCustomHeader(GetConsumerStatusRequestHeader.class);

            Map<MessageQueue, Long> offsetTable = this.mqClientFactory.getConsumerStatus(requestHeader.getTopic(),
                    requestHeader.getGroup());
            GetConsumerStatusBody body = new GetConsumerStatusBody();
            body.setMessageQueueTable(offsetTable);
            response.setBody(body.encode());
            response.setCode(ResponseCode.SUCCESS);
            promise.complete(response);
        } catch (Exception e) {
            promise.fail(e);
        }
        return promise.future();
    }

    private Future<RemotingCommand> getConsumerRunningInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        Promise<RemotingCommand> promise = Promise.promise();
        try {
            final RemotingCommand response = RemotingCommand.createResponseCommand(null);
            final GetConsumerRunningInfoRequestHeader requestHeader = (GetConsumerRunningInfoRequestHeader) request
                    .decodeCommandCustomHeader(GetConsumerRunningInfoRequestHeader.class);

            Future<ConsumerRunningInfo> consumerRunningInfoFuture = this.mqClientFactory
                    .consumerRunningInfo(requestHeader.getConsumerGroup());
            consumerRunningInfoFuture.onFailure(promise::fail);
            consumerRunningInfoFuture.onSuccess(consumerRunningInfo -> {
                if (null != consumerRunningInfo) {
                    if (requestHeader.isJstackEnable()) {
                        Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
                        String jstack = UtilAll.jstack(map);
                        consumerRunningInfo.setJstack(jstack);
                    }

                    response.setCode(ResponseCode.SUCCESS);
                    response.setBody(consumerRunningInfo.encode());
                } else {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark(String.format("The Consumer Group <%s> not exist in this consumer",
                            requestHeader.getConsumerGroup()));
                }
                promise.complete(response);
            });

        } catch (Exception e) {
            promise.fail(e);
        }

        return promise.future();
    }

    private Future<RemotingCommand> consumeMessageDirectly(ChannelHandlerContext ctx, RemotingCommand request) {
        Promise<RemotingCommand> promise = Promise.promise();
        try {
            final RemotingCommand response = RemotingCommand.createResponseCommand(null);
            final ConsumeMessageDirectlyResultRequestHeader requestHeader = (ConsumeMessageDirectlyResultRequestHeader) request
                    .decodeCommandCustomHeader(ConsumeMessageDirectlyResultRequestHeader.class);

            final MessageExt msg = MessageDecoder.decode(ByteBuffer.wrap(request.getBody()));
            Future<ConsumeMessageDirectlyResult> resultFuture = this.mqClientFactory.consumeMessageDirectly(msg,
                    requestHeader.getConsumerGroup(), requestHeader.getBrokerName());
            resultFuture.onFailure(promise::fail);
            resultFuture.onSuccess(result -> {
                if (null != result) {
                    response.setCode(ResponseCode.SUCCESS);
                    response.setBody(result.encode());
                } else {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark(String.format("The Consumer Group <%s> not exist in this consumer",
                            requestHeader.getConsumerGroup()));
                }
                promise.complete(response);
            });

        } catch (Exception e) {
            promise.fail(e);
        }

        return promise.future();
    }

    private Future<RemotingCommand> receiveReplyMessage(ChannelHandlerContext ctx, RemotingCommand request) {

        Promise<RemotingCommand> promise = Promise.promise();
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        try {
            long receiveTime = System.currentTimeMillis();
            ReplyMessageRequestHeader requestHeader = (ReplyMessageRequestHeader) request
                    .decodeCommandCustomHeader(ReplyMessageRequestHeader.class);

            MessageExt msg = new MessageExt();
            msg.setTopic(requestHeader.getTopic());
            msg.setQueueId(requestHeader.getQueueId());
            msg.setStoreTimestamp(requestHeader.getStoreTimestamp());

            if (requestHeader.getBornHost() != null) {
                msg.setBornHost(RemotingUtil.string2SocketAddress(requestHeader.getBornHost()));
            }

            if (requestHeader.getStoreHost() != null) {
                msg.setStoreHost(RemotingUtil.string2SocketAddress(requestHeader.getStoreHost()));
            }

            byte[] body = request.getBody();
            if ((requestHeader.getSysFlag() & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
                try {
                    body = UtilAll.uncompress(body);
                } catch (IOException e) {
                    log.warn("err when uncompress constant", e);
                }
            }
            msg.setBody(body);
            msg.setFlag(requestHeader.getFlag());
            MessageAccessor.setProperties(msg, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REPLY_MESSAGE_ARRIVE_TIME, String.valueOf(receiveTime));
            msg.setBornTimestamp(requestHeader.getBornTimestamp());
            msg.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
            log.debug("receive reply message :{}", msg);

            processReplyMessage(msg);

            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } catch (Exception e) {
            log.warn("unknown err when receiveReplyMsg", e);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("process reply message fail");
        }
        promise.complete(response);
        return promise.future();
    }

    private void processReplyMessage(MessageExt replyMsg) {
        final String correlationId = replyMsg.getUserProperty(MessageConst.PROPERTY_CORRELATION_ID);
        RequestResponseFuture requestResponseFuture = RequestFutureTable.getRequestFutureTable().get(correlationId);
        if (requestResponseFuture != null) {
            RequestFutureTable.getRequestFutureTable().remove(correlationId);
            if (!requestResponseFuture.getPromise().future().isComplete()) {
                requestResponseFuture.getPromise().complete(replyMsg);
            }
        } else {
            String bornHost = replyMsg.getBornHostString();
            log.warn(
                    String.format("receive reply message, but not matched any request, CorrelationId: %s , reply from host: %s",
                            correlationId, bornHost));
        }
    }
}
