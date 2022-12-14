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
package io.vertx.rocketmq.client.trace;

import static io.vertx.rocketmq.client.trace.TraceConstants.TRACE_INSTANCE_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.VertxInternal;
import io.vertx.rocketmq.client.AccessChannel;
import io.vertx.rocketmq.client.common.RocketmqOptions;
import io.vertx.rocketmq.client.common.ThreadLocalIndex;
import io.vertx.rocketmq.client.exception.MQClientException;
import io.vertx.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import io.vertx.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import io.vertx.rocketmq.client.impl.producer.TopicPublishInfo;
import io.vertx.rocketmq.client.log.ClientLogger;
import io.vertx.rocketmq.client.producer.DefaultMQProducer;

public class AsyncTraceDispatcher implements TraceDispatcher {

    private final static InternalLogger log = ClientLogger.getLog();
    private final int batchSize;
    private final int maxMsgSize;
    private final DefaultMQProducer traceProducer;
    private final ThreadPoolExecutor traceExecutor;
    // The last discard number of log
    private final AtomicLong discardCount;
    private final ArrayBlockingQueue<TraceContext> traceContextQueue;
    private final ArrayBlockingQueue<Runnable> appenderQueue;
    private volatile Thread shutDownHook;
    private volatile boolean stopped = false;
    private DefaultMQProducerImpl hostProducer;
    private DefaultMQPushConsumerImpl hostConsumer;
    private final ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    private final String dispatcherId = UUID.randomUUID().toString();
    private String traceTopicName;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private AccessChannel accessChannel = AccessChannel.LOCAL;
    private final String group;
    private final Type type;

    public AsyncTraceDispatcher(String group, Type type, String traceTopicName, VertxInternal vertx, RocketmqOptions options) {
        // queueSize is greater than or equal to the n power of 2 of value
        int queueSize = 2048;
        this.batchSize = 100;
        this.maxMsgSize = 128000;
        this.discardCount = new AtomicLong(0L);
        this.traceContextQueue = new ArrayBlockingQueue<>(1024);
        this.group = group;
        this.type = type;

        this.appenderQueue = new ArrayBlockingQueue<>(queueSize);
        if (!UtilAll.isBlank(traceTopicName)) {
            this.traceTopicName = traceTopicName;
        } else {
            this.traceTopicName = TopicValidator.RMQ_SYS_TRACE_TOPIC;
        }
        this.traceExecutor = new ThreadPoolExecutor(//
                10, //
                20, //
                1000 * 60, //
                TimeUnit.MILLISECONDS, //
                this.appenderQueue, //
                new ThreadFactoryImpl("MQTraceSendThread_"));
        traceProducer = getAndCreateTraceProducer(null, vertx, options);
    }

    public AccessChannel getAccessChannel() {
        return accessChannel;
    }

    public void setAccessChannel(AccessChannel accessChannel) {
        this.accessChannel = accessChannel;
    }

    public String getTraceTopicName() {
        return traceTopicName;
    }

    public void setTraceTopicName(String traceTopicName) {
        this.traceTopicName = traceTopicName;
    }

    public DefaultMQProducer getTraceProducer() {
        return traceProducer;
    }

    public DefaultMQProducerImpl getHostProducer() {
        return hostProducer;
    }

    public void setHostProducer(DefaultMQProducerImpl hostProducer) {
        this.hostProducer = hostProducer;
    }

    public DefaultMQPushConsumerImpl getHostConsumer() {
        return hostConsumer;
    }

    public void setHostConsumer(DefaultMQPushConsumerImpl hostConsumer) {
        this.hostConsumer = hostConsumer;
    }

    public void start(String nameSrvAddr, AccessChannel accessChannel) throws MQClientException {
        if (isStarted.compareAndSet(false, true)) {
            traceProducer.setNamesrvAddr(nameSrvAddr);
            traceProducer.setInstanceName(TRACE_INSTANCE_NAME + "_" + nameSrvAddr);
            traceProducer.start();
        }
        this.accessChannel = accessChannel;
        Thread worker = new Thread(new AsyncRunnable(), "MQ-AsyncTraceDispatcher-Thread-" + dispatcherId);
        worker.setDaemon(true);
        worker.start();
        this.registerShutDownHook();
    }

    private DefaultMQProducer getAndCreateTraceProducer(RPCHook rpcHook, VertxInternal vertx, RocketmqOptions options) {
        DefaultMQProducer traceProducerInstance = this.traceProducer;
        if (traceProducerInstance == null) {
            traceProducerInstance = new DefaultMQProducer(vertx, options);
            traceProducerInstance.setProducerGroup(genGroupNameForTrace());
            traceProducerInstance.setSendMsgTimeout(5000);
            traceProducerInstance.setVipChannelEnabled(false);
            // The max size of message is 128K
            traceProducerInstance.setMaxMessageSize(maxMsgSize - 10 * 1000);
        }
        return traceProducerInstance;
    }

    private String genGroupNameForTrace() {
        return TraceConstants.GROUP_NAME_PREFIX + "-" + this.group + "-" + this.type;
    }

    @Override
    public boolean append(final Object ctx) {
        boolean result = traceContextQueue.offer((TraceContext) ctx);
        if (!result) {
            log.info("buffer full" + discardCount.incrementAndGet() + " ,context is " + ctx);
        }
        return result;
    }

    @Override
    public void flush() throws IOException {
        // The maximum waiting time for refresh,avoid being written all the time, resulting in failure to return.
        long end = System.currentTimeMillis() + 500;
        while (traceContextQueue.size() > 0 || appenderQueue.size() > 0 && System.currentTimeMillis() <= end) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                break;
            }
        }
        log.info("------end trace send " + traceContextQueue.size() + "   " + appenderQueue.size());
    }

    @Override
    public void shutdown() {
        this.stopped = true;
        this.traceExecutor.shutdown();
        if (isStarted.get()) {
            traceProducer.shutdown();
        }
        this.removeShutdownHook();
    }

    public void registerShutDownHook() {
        if (shutDownHook == null) {
            shutDownHook = new Thread(new Runnable() {

                @Override
                public void run() {
                    synchronized (this) {
                        try {
                            flush();
                        } catch (IOException e) {
                            log.error("system MQTrace hook shutdown failed ,maybe loss some trace data");
                        }
                    }
                }
            }, "ShutdownHookMQTrace");
            Runtime.getRuntime().addShutdownHook(shutDownHook);
        }
    }

    public void removeShutdownHook() {
        if (shutDownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutDownHook);
            } catch (IllegalStateException e) {
                // ignore - VM is already shutting down
            }
        }
    }

    class AsyncRunnable implements Runnable {
        private boolean stopped;

        @Override
        public void run() {
            while (!stopped) {
                List<TraceContext> contexts = new ArrayList<>(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    TraceContext context = null;
                    try {
                        //get trace data element from blocking Queue ??? traceContextQueue
                        context = traceContextQueue.poll(5, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ignored) {
                    }
                    if (context != null) {
                        contexts.add(context);
                    } else {
                        break;
                    }
                }
                if (contexts.size() > 0) {
                    AsyncAppenderRequest request = new AsyncAppenderRequest(contexts);
                    traceExecutor.submit(request);
                } else if (AsyncTraceDispatcher.this.stopped) {
                    this.stopped = true;
                }
            }

        }
    }

    class AsyncAppenderRequest implements Runnable {
        List<TraceContext> contextList;

        public AsyncAppenderRequest(final List<TraceContext> contextList) {
            this.contextList = Objects.requireNonNullElseGet(contextList, () -> new ArrayList<>(1));
        }

        @Override
        public void run() {
            sendTraceData(contextList);
        }

        public void sendTraceData(List<TraceContext> contextList) {
            Map<String, List<TraceTransferBean>> transBeanMap = new HashMap<>();
            for (TraceContext context : contextList) {
                if (context.getTraceBeans().isEmpty()) {
                    continue;
                }
                // Topic value corresponding to original message entity content
                String topic = context.getTraceBeans().get(0).getTopic();
                String regionId = context.getRegionId();
                // Use  original message entity's topic as key
                String key = topic;
                if (!StringUtils.isBlank(regionId)) {
                    key = key + TraceConstants.CONTENT_SPLITOR + regionId;
                }
                List<TraceTransferBean> transBeanList = transBeanMap.computeIfAbsent(key, k -> new ArrayList<>());
                TraceTransferBean traceData = TraceDataEncoder.encoderFromContextBean(context);
                transBeanList.add(traceData);
            }
            for (Map.Entry<String, List<TraceTransferBean>> entry : transBeanMap.entrySet()) {
                String[] key = entry.getKey().split(String.valueOf(TraceConstants.CONTENT_SPLITOR));
                String regionId = null;
                if (key.length > 1) {
                    regionId = key[1];
                }
                flushData(entry.getValue(), regionId);
            }
        }

        /**
         * Batch sending data actually
         */
        private void flushData(List<TraceTransferBean> transBeanList, String regionId) {
            if (transBeanList.size() == 0) {
                return;
            }
            // Temporary buffer
            StringBuilder buffer = new StringBuilder(1024);
            int count = 0;
            Set<String> keySet = new HashSet<>();

            for (TraceTransferBean bean : transBeanList) {
                // Keyset of message trace includes msgId of or original message
                keySet.addAll(bean.getTransKey());
                buffer.append(bean.getTransData());
                count++;
                // Ensure that the size of the package should not exceed the upper limit.
                if (buffer.length() >= traceProducer.getMaxMessageSize()) {
                    sendTraceDataByMQ(keySet, buffer.toString(), regionId);
                    // Clear temporary buffer after finishing
                    buffer.delete(0, buffer.length());
                    keySet.clear();
                    count = 0;
                }
            }
            if (count > 0) {
                sendTraceDataByMQ(keySet, buffer.toString(), regionId);
            }
            transBeanList.clear();
        }

        /**
         * Send message trace data
         *
         * @param keySet the keyset in this batch(including msgId in original message not offsetMsgId)
         * @param data the message trace data in this batch
         */
        private Future<Void> sendTraceDataByMQ(Set<String> keySet, final String data, String regionId) {
            String traceTopic = traceTopicName;
            if (AccessChannel.CLOUD == accessChannel) {
                traceTopic = TraceConstants.TRACE_TOPIC_PREFIX + regionId;
            }
            final Message message = new Message(traceTopic, data.getBytes());
            // Keyset of message trace includes msgId of or original message
            message.setKeys(keySet);
            Promise<Void> promise = Promise.promise();

            Future<Set<String>> traceBrokerSetFuture = tryGetMessageQueueBrokerSet(traceProducer.getDefaultMQProducerImpl(),
                    traceTopic);
            traceBrokerSetFuture.onComplete(ar -> {
                if (ar.failed()) {
                    log.info("send trace data,the traceData is" + data);
                    return;
                }
                Set<String> traceBrokerSet = ar.result();
                if (traceBrokerSet.isEmpty()) {
                    traceProducer.send(message, 5000).onFailure(e -> log.info("send trace data ,the traceData is " + data));
                } else {
                    traceProducer.send(message, (mqs, msg, arg) -> {
                        Set<String> brokerSet = (Set<String>) arg;
                        List<MessageQueue> filterMqs = new ArrayList<>();
                        for (MessageQueue queue : mqs) {
                            if (brokerSet.contains(queue.getBrokerName())) {
                                filterMqs.add(queue);
                            }
                        }
                        int index = sendWhichQueue.getAndIncrement();
                        int pos = Math.abs(index) % filterMqs.size();
                        if (pos < 0) {
                            pos = 0;
                        }
                        return filterMqs.get(pos);
                    }, traceBrokerSet).onFailure(e -> log.info("send trace data ,the traceData is " + data));
                }
                promise.complete();
            });

            return promise.future();
        }

        private Future<Set<String>> tryGetMessageQueueBrokerSet(DefaultMQProducerImpl producer, String topic) {
            Set<String> brokerSet = new HashSet<>();
            TopicPublishInfo topicPublishInfo = producer.getTopicPublishInfoTable().get(topic);
            Promise<Set<String>> promise = Promise.promise();
            Promise<TopicPublishInfo> brokerSetPromise = Promise.promise();
            if (null == topicPublishInfo || !topicPublishInfo.ok()) {
                producer.getTopicPublishInfoTable().putIfAbsent(topic, new TopicPublishInfo());
                Future<Boolean> booleanFuture = producer.getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);
                booleanFuture.onFailure(brokerSetPromise::fail);
                booleanFuture.onSuccess(bool -> brokerSetPromise.complete(producer.getTopicPublishInfoTable().get(topic)));
            } else {
                brokerSetPromise.complete(topicPublishInfo);
            }
            brokerSetPromise.future().onFailure(promise::fail).onSuccess(tpi -> {
                if (tpi.isHaveTopicRouterInfo() || tpi.ok()) {
                    for (MessageQueue queue : tpi.getMessageQueueList()) {
                        brokerSet.add(queue.getBrokerName());
                    }
                }
                promise.complete(brokerSet);
            });

            return promise.future();
        }
    }

}
