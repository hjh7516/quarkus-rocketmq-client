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
package io.vertx.rocketmq.client.consumer;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import io.vertx.core.Future;
import io.vertx.rocketmq.client.exception.MQClientException;

public interface LitePullConsumer {

    /**
     * Start the consumer
     */
    void start() throws MQClientException;

    /**
     * Shutdown the consumer
     */
    void shutdown();

    /**
     * Subscribe some topic with subExpression
     *
     * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br>
     *        if
     *        null or * expression,meaning subscribe all
     */
    Future<Void> subscribe(final String topic, final String subExpression);

    /**
     * Subscribe some topic with selector.
     *
     * @param selector message selector({@link MessageSelector}), can be null.
     */
    Future<Void> subscribe(final String topic, final MessageSelector selector);

    /**
     * Unsubscribe consumption some topic
     *
     * @param topic Message topic that needs to be unsubscribe.
     */
    void unsubscribe(final String topic);

    /**
     * Manually assign a list of message queues to this consumer. This interface does not allow for incremental
     * assignment and will replace the previous assignment (if there is one).
     *
     * @param messageQueues Message queues that needs to be assigned.
     */
    void assign(Collection<MessageQueue> messageQueues);

    /**
     * Fetch data for the topics or partitions specified using assign API
     *
     * @return list of message, can be null.
     */
    Future<List<MessageExt>> poll();

    /**
     * Fetch data for the topics or partitions specified using assign API
     *
     * @param timeout The amount time, in milliseconds, spent waiting in poll if data is not available. Must not be
     *        negative
     * @return list of message, can be null.
     */
    Future<List<MessageExt>> poll(long timeout);

    /**
     * Overrides the fetch offsets that the consumer will use on the next poll. If this API is invoked for the same
     * message queue more than once, the latest offset will be used on the next poll(). Note that you may lose data if
     * this API is arbitrarily used in the middle of consumption.
     *
     * @param messageQueue
     * @param offset
     */
    Future<Void> seek(MessageQueue messageQueue, long offset);

    /**
     * Suspend pulling from the requested message queues.
     *
     * Because of the implementation of pre-pull, fetch data in {@link #poll()} will not stop immediately until the
     * messages of the requested message queues drain.
     *
     * Note that this method does not affect message queue subscription. In particular, it does not cause a group
     * rebalance.
     *
     * @param messageQueues Message queues that needs to be paused.
     */
    void pause(Collection<MessageQueue> messageQueues);

    /**
     * Resume specified message queues which have been paused with {@link #pause(Collection)}.
     *
     * @param messageQueues Message queues that needs to be resumed.
     */
    void resume(Collection<MessageQueue> messageQueues);

    /**
     * Whether to enable auto-commit consume offset.
     *
     * @return true if enable auto-commit, false if disable auto-commit.
     */
    boolean isAutoCommit();

    /**
     * Set whether to enable auto-commit consume offset.
     *
     * @param autoCommit Whether to enable auto-commit.
     */
    void setAutoCommit(boolean autoCommit);

    /**
     * Get metadata about the message queues for a given topic.
     *
     * @param topic The topic that need to get metadata.
     * @return collection of message queues
     */
    Future<Set<MessageQueue>> fetchMessageQueues(String topic);

    /**
     * Look up the offsets for the given message queue by timestamp. The returned offset for each message queue is the
     * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding message
     * queue.
     *
     * @param messageQueue Message queues that needs to get offset by timestamp.
     * @param timestamp
     * @return offset
     * @throws MQClientException if there is any client error.
     */
    Future<Long> offsetForTimestamp(MessageQueue messageQueue, Long timestamp);

    /**
     * Manually commit consume offset.
     */
    Future<Void> commitSync();

    /**
     * Get the last committed offset for the given message queue.
     *
     * @param messageQueue
     * @return offset, if offset equals -1 means no offset in broker.
     */
    Future<Long> committed(MessageQueue messageQueue);

    /**
     * Register a callback for sensing topic metadata changes.
     *
     * @param topic The topic that need to monitor.
     * @param topicMessageQueueChangeListener Callback when topic metadata changes, refer {@link
     *        TopicMessageQueueChangeListener}
     * @throws MQClientException if there is any client error.
     */
    void registerTopicMessageQueueChangeListener(String topic,
            TopicMessageQueueChangeListener topicMessageQueueChangeListener) throws MQClientException;

    /**
     * Update name server addresses.
     */
    void updateNameServerAddress(String nameServerAddress);

    /**
     * Overrides the fetch offsets with the begin offset that the consumer will use on the next poll. If this API is
     * invoked for the same message queue more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption.
     *
     */
    Future<Void> seekToBegin(MessageQueue messageQueue);

    /**
     * Overrides the fetch offsets with the end offset that the consumer will use on the next poll. If this API is
     * invoked for the same message queue more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption.
     *
     */
    Future<Void> seekToEnd(MessageQueue messageQueue);
}
