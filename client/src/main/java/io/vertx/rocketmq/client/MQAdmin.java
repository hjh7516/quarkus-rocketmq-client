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
package io.vertx.rocketmq.client;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import io.vertx.core.Future;

/**
 * Base interface for MQ management
 */
public interface MQAdmin {
    /**
     * Creates an topic
     *
     * @param key accesskey
     * @param newTopic topic name
     * @param queueNum topic's queue number
     */
    Future<Void> createTopic(final String key, final String newTopic, final int queueNum);

    /**
     * Creates an topic
     *
     * @param key accesskey
     * @param newTopic topic name
     * @param queueNum topic's queue number
     * @param topicSysFlag topic system flag
     */
    Future<Void> createTopic(String key, String newTopic, int queueNum, int topicSysFlag);

    /**
     * Gets the message queue offset according to some time in milliseconds<br>
     * be cautious to call because of more IO overhead
     *
     * @param mq Instance of MessageQueue
     * @param timestamp from when in milliseconds.
     * @return offset
     */
    Future<Long> searchOffset(final MessageQueue mq, final long timestamp);

    /**
     * Gets the max offset
     *
     * @param mq Instance of MessageQueue
     * @return the max offset
     */
    Future<Long> maxOffset(final MessageQueue mq);

    /**
     * Gets the minimum offset
     *
     * @param mq Instance of MessageQueue
     * @return the minimum offset
     */
    Future<Long> minOffset(final MessageQueue mq);

    /**
     * Gets the earliest stored message time
     *
     * @param mq Instance of MessageQueue
     * @return the time in microseconds
     */
    Future<Long> earliestMsgStoreTime(final MessageQueue mq);

    /**
     * Query message according to message id
     *
     * @param offsetMsgId message id
     * @return message
     */
    Future<MessageExt> viewMessage(final String offsetMsgId);

    /**
     * Query messages
     *
     * @param topic message topic
     * @param key message key index word
     * @param maxNum max message number
     * @param begin from when
     * @param end to when
     * @return Instance of QueryResult
     */
    Future<QueryResult> queryMessage(final String topic, final String key, final int maxNum, final long begin,
            final long end);

    /**
     * @return The {@code MessageExt} of given msgId
     */
    Future<MessageExt> viewMessage(String topic, String msgId);

}
