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

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.rocketmq.client.ReactiveUtil;
import io.vertx.rocketmq.client.consumer.DefaultMQPushConsumer;
import io.vertx.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import io.vertx.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import io.vertx.rocketmq.client.consumer.listener.ConsumeReturnType;
import io.vertx.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import io.vertx.rocketmq.client.log.ClientLogger;
import io.vertx.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerConcurrently messageListener;
    private final ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;

    private final ScheduledExecutorService scheduledExecutorService;
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
            MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        BlockingQueue<Runnable> consumeRequestQueue = new LinkedBlockingQueue<>();

        this.consumeExecutor = new ThreadPoolExecutor(
                this.defaultMQPushConsumer.getConsumeThreadMin(),
                this.defaultMQPushConsumer.getConsumeThreadMax(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                consumeRequestQueue,
                new ThreadFactoryImpl("ConsumeMessageThread_"));

        this.scheduledExecutorService = Executors
                .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
        this.cleanExpireMsgExecutors = Executors
                .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));
    }

    public void start() {
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(this::cleanExpireMsg, this.defaultMQPushConsumer.getConsumeTimeout(),
                this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
    }

    public void shutdown(long awaitTerminateMillis) {
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        this.cleanExpireMsgExecutors.shutdown();
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
                && corePoolSize <= Short.MAX_VALUE
                && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
    }

    @Override
    public void decCorePoolSize() {
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public Future<ConsumeMessageDirectlyResult> consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        List<MessageExt> msgs = new ArrayList<>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);
        Promise<ConsumeMessageDirectlyResult> promise = Promise.promise();
        Future<ConsumeConcurrentlyStatus> statusFuture = this.messageListener.consumeMessage(msgs, context);
        statusFuture.onComplete(ar -> {
            if (ar.succeeded()) {
                ConsumeConcurrentlyStatus status = ar.result();
                if (status != null) {
                    switch (status) {
                        case CONSUME_SUCCESS:
                            result.setConsumeResult(CMResult.CR_SUCCESS);
                            break;
                        case RECONSUME_LATER:
                            result.setConsumeResult(CMResult.CR_LATER);
                            break;
                        default:
                            break;
                    }
                } else {
                    result.setConsumeResult(CMResult.CR_RETURN_NULL);
                }
            } else {
                result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
                result.setRemark(RemotingHelper.exceptionSimpleDesc(ar.cause()));

                log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                        RemotingHelper.exceptionSimpleDesc(ar.cause()),
                        ConsumeMessageConcurrentlyService.this.consumerGroup,
                        msgs,
                        mq), ar.cause());
            }
            result.setSpentTimeMills(System.currentTimeMillis() - beginTime);
            log.info("consumeMessageDirectly Result: {}", result);
            promise.complete(result);
        });
        return promise.future();
    }

    @Override
    public void submitConsumeRequest(
            final List<MessageExt> msgs,
            final ProcessQueue processQueue,
            final MessageQueue messageQueue,
            final boolean dispatchToConsume) {
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        if (msgs.size() <= consumeBatchSize) {
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                this.submitConsumeRequestLater(consumeRequest);
            }
        } else {
            for (int total = 0; total < msgs.size();) {
                List<MessageExt> msgThis = new ArrayList<>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }
                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }

                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }

    private void cleanExpireMsg() {
        for (Map.Entry<MessageQueue, ProcessQueue> next : this.defaultMQPushConsumerImpl.getRebalanceImpl()
                .getProcessQueueTable().entrySet()) {
            ProcessQueue pq = next.getValue();
            pq.cleanExpiredMsg(this.defaultMQPushConsumer);
        }
    }

    public void processConsumeResult(
            final ConsumeConcurrentlyStatus status,
            final ConsumeConcurrentlyContext context,
            final ConsumeRequest consumeRequest) {
        int ackIndex = context.getAckIndex();

        if (consumeRequest.getMsgs().isEmpty())
            return;

        switch (status) {
            case CONSUME_SUCCESS:
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }
                int ok = ackIndex + 1;
                int failed = consumeRequest.getMsgs().size() - ok;
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                        failed);
                break;
            case RECONSUME_LATER:
                ackIndex = -1;
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                        consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }

        switch (this.defaultMQPushConsumer.getMessageModel()) {
            case BROADCASTING:
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;
            case CLUSTERING:
                List<MessageExt> msgBackFailed = new ArrayList<>(consumeRequest.getMsgs().size());
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    boolean result = ReactiveUtil.waitOne(this.sendMessageBack(msg, context));
                    if (!result) {
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        msgBackFailed.add(msg);
                    }
                }

                if (!msgBackFailed.isEmpty()) {
                    consumeRequest.getMsgs().removeAll(msgBackFailed);

                    this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(),
                            consumeRequest.getMessageQueue());
                }
                break;
            default:
                break;
        }

        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());
        if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    public Future<Boolean> sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {
        int delayLevel = context.getDelayLevelWhenNextConsume();

        // Wrap topic with namespace before sending back message.
        msg.setTopic(this.defaultMQPushConsumer.withNamespace(msg.getTopic()));
        Promise<Boolean> promise = Promise.promise();
        this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, context.getMessageQueue().getBrokerName())
                .onComplete(ar -> {
                    if (ar.failed()) {
                        log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg, ar.cause());
                        promise.complete(Boolean.FALSE);
                    } else {
                        promise.complete(Boolean.TRUE);
                    }
                });

        return promise.future();
    }

    private void submitConsumeRequestLater(
            final List<MessageExt> msgs,
            final ProcessQueue processQueue,
            final MessageQueue messageQueue) {

        this.scheduledExecutorService.schedule(
                () -> ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue, true), 5000,
                TimeUnit.MILLISECONDS);
    }

    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest) {

        this.scheduledExecutorService.schedule(() -> {
            ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
        }, 5000, TimeUnit.MILLISECONDS);
    }

    class ConsumeRequest implements Runnable {
        private final List<MessageExt> msgs;
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        @Override
        public void run() {
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}",
                        ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }
            Promise<Void> promise = Promise.promise();
            promise.complete();
            Future<Void> future = promise.future();

            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());
            future = future.compose(
                    v -> ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(messageQueue, msgs))
                    .compose(consumeMessageContext -> {
                        long beginTimestamp = System.currentTimeMillis();
                        if (!msgs.isEmpty()) {
                            for (MessageExt msg : msgs) {
                                MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                            }
                        }
                        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
                        Future<ConsumeConcurrentlyStatus> exeRsFuture = Vertx.currentContext()
                                .executeBlocking(blockPromise -> ConsumeMessageConcurrentlyService.this.messageListener
                                        .consumeMessage(Collections.unmodifiableList(msgs), context)
                                        .onFailure(blockPromise::fail).onSuccess(blockPromise::complete));
                        return exeRsFuture.compose(status -> {
                            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;

                            long consumeRT = System.currentTimeMillis() - beginTimestamp;
                            if (null == status) {
                                returnType = ConsumeReturnType.RETURNNULL;
                            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                                returnType = ConsumeReturnType.TIME_OUT;
                            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                                returnType = ConsumeReturnType.FAILED;
                            }

                            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                            }
                            if (null == status) {
                                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                                        ConsumeMessageConcurrentlyService.this.consumerGroup,
                                        msgs,
                                        messageQueue);
                                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
                            }
                            consumeMessageContext.setStatus(status.toString());
                            consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                            ConsumeConcurrentlyStatus finalStatus = status;
                            return ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl
                                    .executeHookAfter(consumeMessageContext)
                                    .compose(v -> {
                                        ConsumeMessageConcurrentlyService.this.getConsumerStatsManager()
                                                .incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup,
                                                        messageQueue.getTopic(), consumeRT);

                                        if (!processQueue.isDropped()) {
                                            ConsumeMessageConcurrentlyService.this.processConsumeResult(finalStatus,
                                                    context, this);
                                        } else {
                                            log.warn(
                                                    "processQueue is dropped without process consume result. messageQueue={}, msgs={}",
                                                    messageQueue, msgs);
                                        }
                                        Promise<Void> innerPromise = Promise.promise();
                                        innerPromise.complete();
                                        return innerPromise.future();
                                    });
                        });
                    });

            future.onFailure(e -> log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue));
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }
}
