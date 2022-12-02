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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.rocketmq.client.ReactiveUtil;
import io.vertx.rocketmq.client.consumer.DefaultMQPushConsumer;
import io.vertx.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import io.vertx.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import io.vertx.rocketmq.client.consumer.listener.ConsumeReturnType;
import io.vertx.rocketmq.client.consumer.listener.MessageListenerOrderly;
import io.vertx.rocketmq.client.hook.ConsumeMessageContext;
import io.vertx.rocketmq.client.log.ClientLogger;
import io.vertx.rocketmq.client.stat.ConsumerStatsManager;

public class ConsumeMessageOrderlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY = Long
            .parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final MessageListenerOrderly messageListener;
    private final ThreadPoolExecutor consumeExecutor;
    private final String consumerGroup;
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean stopped = false;

    public ConsumeMessageOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
            MessageListenerOrderly messageListener) {
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
    }

    public void start() {
        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())) {
            this.scheduledExecutorService.scheduleAtFixedRate(ConsumeMessageOrderlyService.this::lockMQPeriodically, 1000,
                    ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown(long awaitTerminateMillis) {
        this.stopped = true;
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            this.unlockAllMQ();
        }
    }

    public synchronized void unlockAllMQ() {
        this.defaultMQPushConsumerImpl.getRebalanceImpl().unlockAll(false);
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
        result.setOrder(true);

        List<MessageExt> msgs = new ArrayList<>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeOrderlyContext context = new ConsumeOrderlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        Promise<ConsumeMessageDirectlyResult> promise = Promise.promise();
        Future<ConsumeOrderlyStatus> statusFuture = this.messageListener.consumeMessage(msgs, context);
        statusFuture.onFailure(promise::fail);
        statusFuture.onComplete(ar -> {
            if (ar.succeeded()) {
                ConsumeOrderlyStatus status = ar.result();
                if (status != null) {
                    switch (status) {
                        case COMMIT:
                            result.setConsumeResult(CMResult.CR_COMMIT);
                            break;
                        case ROLLBACK:
                            result.setConsumeResult(CMResult.CR_ROLLBACK);
                            break;
                        case SUCCESS:
                            result.setConsumeResult(CMResult.CR_SUCCESS);
                            break;
                        case SUSPEND_CURRENT_QUEUE_A_MOMENT:
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
                        ConsumeMessageOrderlyService.this.consumerGroup,
                        msgs,
                        mq), ar.cause());
            }
            result.setAutoCommit(context.isAutoCommit());
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
            final boolean dispathToConsume) {
        if (dispathToConsume) {
            ConsumeRequest consumeRequest = new ConsumeRequest(processQueue, messageQueue);
            this.consumeExecutor.submit(consumeRequest);
        }
    }

    public void lockMQPeriodically() {
        if (!this.stopped) {
            this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
        }
    }

    public void tryLockLaterAndReconsume(final MessageQueue mq, final ProcessQueue processQueue,
            final long delayMills) {
        this.scheduledExecutorService.schedule(() -> {
            Future<Boolean> lockOKFuture = ConsumeMessageOrderlyService.this.lockOneMQ(mq);
            lockOKFuture.onFailure(e -> log.error("", e));
            lockOKFuture.onSuccess(lockOK -> {
                if (lockOK) {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 10);
                } else {
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 3000);
                }
            });
        }, delayMills, TimeUnit.MILLISECONDS);
    }

    public Future<Boolean> lockOneMQ(final MessageQueue mq) {

        if (!this.stopped) {
            return this.defaultMQPushConsumerImpl.getRebalanceImpl().lock(mq);
        }
        Promise<Boolean> promise = Promise.promise();
        promise.complete(Boolean.FALSE);
        return promise.future();
    }

    private void submitConsumeRequestLater(
            final ProcessQueue processQueue,
            final MessageQueue messageQueue,
            final long suspendTimeMillis) {
        long timeMillis = suspendTimeMillis;
        if (timeMillis == -1) {
            timeMillis = this.defaultMQPushConsumer.getSuspendCurrentQueueTimeMillis();
        }

        if (timeMillis < 10) {
            timeMillis = 10;
        } else if (timeMillis > 30000) {
            timeMillis = 30000;
        }

        this.scheduledExecutorService.schedule(
                () -> ConsumeMessageOrderlyService.this.submitConsumeRequest(null, processQueue, messageQueue, true),
                timeMillis, TimeUnit.MILLISECONDS);
    }

    public boolean processConsumeResult(
            final List<MessageExt> msgs,
            final ConsumeOrderlyStatus status,
            final ConsumeOrderlyContext context,
            final ConsumeRequest consumeRequest) {
        boolean continueConsume = true;
        long commitOffset = -1L;
        if (context.isAutoCommit()) {
            switch (status) {
                case COMMIT:
                case ROLLBACK:
                    log.warn("the message queue consume result is illegal, we think you want to ack these message {}",
                            consumeRequest.getMessageQueue());
                case SUCCESS:
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                            msgs.size());
                    break;
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup,
                            consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    if (checkReconsumeTimes(msgs)) {
                        consumeRequest.getProcessQueue().makeMessageToCosumeAgain(msgs);
                        this.submitConsumeRequestLater(
                                consumeRequest.getProcessQueue(),
                                consumeRequest.getMessageQueue(),
                                context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                    } else {
                        commitOffset = consumeRequest.getProcessQueue().commit();
                    }
                    break;
                default:
                    break;
            }
        } else {
            switch (status) {
                case SUCCESS:
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                            msgs.size());
                    break;
                case COMMIT:
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    break;
                case ROLLBACK:
                    consumeRequest.getProcessQueue().rollback();
                    this.submitConsumeRequestLater(
                            consumeRequest.getProcessQueue(),
                            consumeRequest.getMessageQueue(),
                            context.getSuspendCurrentQueueTimeMillis());
                    continueConsume = false;
                    break;
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup,
                            consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    if (checkReconsumeTimes(msgs)) {
                        consumeRequest.getProcessQueue().makeMessageToCosumeAgain(msgs);
                        this.submitConsumeRequestLater(
                                consumeRequest.getProcessQueue(),
                                consumeRequest.getMessageQueue(),
                                context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                    }
                    break;
                default:
                    break;
            }
        }

        if (commitOffset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), commitOffset, false);
        }

        return continueConsume;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    private int getMaxReconsumeTimes() {
        // default reconsume times: Integer.MAX_VALUE
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return Integer.MAX_VALUE;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    private boolean checkReconsumeTimes(List<MessageExt> msgs) {
        boolean suspend = false;
        if (msgs != null && !msgs.isEmpty()) {
            for (MessageExt msg : msgs) {
                if (msg.getReconsumeTimes() >= getMaxReconsumeTimes()) {
                    MessageAccessor.setReconsumeTime(msg, String.valueOf(msg.getReconsumeTimes()));
                    if (!sendMessageBack(msg)) {
                        suspend = true;
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                    }
                } else {
                    suspend = true;
                    msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                }
            }
        }
        return suspend;
    }

    public boolean sendMessageBack(final MessageExt msg) {
        try {
            // max reconsume times exceeded then send to dead letter queue.
            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes()));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

            ReactiveUtil.waitOne(this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory()
                    .getDefaultMQProducer().send(newMsg));
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    public void resetNamespace(final List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    class ConsumeRequest implements Runnable {
        private final ProcessQueue processQueue;
        private final MessageQueue messageQueue;

        public ConsumeRequest(ProcessQueue processQueue, MessageQueue messageQueue) {
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        //顺序消息消费线程
        @Override
        public void run() {
            if (this.processQueue.isDropped()) {
                log.warn("run, the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                return;
            }

            final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);
            synchronized (objLock) {//通过加锁，将并发的消息顺序进行消费。消息处理的方式没什么特别。
                if (MessageModel.BROADCASTING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                        || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {
                    final long beginTime = System.currentTimeMillis();
                    for (boolean continueConsume = true; continueConsume;) {
                        if (this.processQueue.isDropped()) {
                            log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                            break;
                        }

                        if (MessageModel.CLUSTERING
                                .equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                                && !this.processQueue.isLocked()) {
                            log.warn("the message queue not locked, so consume later, {}", this.messageQueue);
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue,
                                    10);
                            break;
                        }

                        if (MessageModel.CLUSTERING
                                .equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                                && this.processQueue.isLockExpired()) {
                            log.warn("the message queue lock expired, so consume later, {}", this.messageQueue);
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue,
                                    10);
                            break;
                        }

                        long interval = System.currentTimeMillis() - beginTime;
                        if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                            ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, messageQueue, 10);
                            break;
                        }

                        final int consumeBatchSize = ConsumeMessageOrderlyService.this.defaultMQPushConsumer
                                .getConsumeMessageBatchMaxSize();

                        List<MessageExt> msgs = this.processQueue.takeMessages(consumeBatchSize);
                        defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());
                        if (!msgs.isEmpty()) {
                            final ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);
                            ConsumeOrderlyStatus status = null;

                            ConsumeMessageContext consumeMessageContext = null;
                            ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookBefore(messageQueue, msgs);

                            long beginTimestamp = System.currentTimeMillis();
                            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                            boolean hasException = false;
                            try {
                                this.processQueue.getLockConsume().lock();
                                if (this.processQueue.isDropped()) {
                                    log.warn(
                                            "consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                                            this.messageQueue);
                                    break;
                                }

                                status = ReactiveUtil
                                        .waitOne(messageListener.consumeMessage(Collections.unmodifiableList(msgs), context));
                            } catch (Throwable e) {
                                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                                        RemotingHelper.exceptionSimpleDesc(e),
                                        ConsumeMessageOrderlyService.this.consumerGroup,
                                        msgs,
                                        messageQueue);
                                hasException = true;
                            } finally {
                                this.processQueue.getLockConsume().unlock();
                            }

                            if (null == status
                                    || ConsumeOrderlyStatus.ROLLBACK == status
                                    || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                log.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                                        ConsumeMessageOrderlyService.this.consumerGroup,
                                        msgs,
                                        messageQueue);
                            }

                            long consumeRT = System.currentTimeMillis() - beginTimestamp;
                            if (null == status) {
                                if (hasException) {
                                    returnType = ConsumeReturnType.EXCEPTION;
                                } else {
                                    returnType = ConsumeReturnType.RETURNNULL;
                                }
                            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                                returnType = ConsumeReturnType.TIME_OUT;
                            } else if (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                returnType = ConsumeReturnType.FAILED;
                            }

                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                            }

                            if (null == status) {
                                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                            }

                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.setStatus(status.toString());
                                consumeMessageContext
                                        .setSuccess(ConsumeOrderlyStatus.SUCCESS == status
                                                || ConsumeOrderlyStatus.COMMIT == status);
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl
                                        .executeHookAfter(consumeMessageContext);
                            }

                            ConsumeMessageOrderlyService.this.getConsumerStatsManager()
                                    .incConsumeRT(ConsumeMessageOrderlyService.this.consumerGroup, messageQueue.getTopic(),
                                            consumeRT);

                            continueConsume = ConsumeMessageOrderlyService.this.processConsumeResult(msgs, status, context,
                                    this);
                        } else {
                            continueConsume = false;
                        }
                    }
                } else {
                    if (this.processQueue.isDropped()) {
                        log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                        return;
                    }

                    ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
                }
            }
        }

    }

}
