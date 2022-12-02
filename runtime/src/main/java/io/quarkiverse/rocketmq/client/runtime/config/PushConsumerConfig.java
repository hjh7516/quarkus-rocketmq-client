package io.quarkiverse.rocketmq.client.runtime.config;

import java.util.Map;
import java.util.Optional;

import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import io.quarkus.runtime.annotations.ConfigGroup;
import io.quarkus.runtime.annotations.ConfigItem;
import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;

@ConfigRoot(phase = ConfigPhase.BUILD_AND_RUN_TIME_FIXED, name = PushConsumerConfig.CONSUMER_CONFIG_ROOT_NAME)
public class PushConsumerConfig {

    final static String CONSUMER_CONFIG_ROOT_NAME = "rocketmq.push-consumer";

    /**
     * The default config.
     */
    @ConfigItem(name = ConfigItem.PARENT)
    public PushConfiguration defaultConfig;

    /**
     * Additional named config.
     */
    @ConfigItem(name = ConfigItem.PARENT)
    public Map<String, PushConfiguration> additionalConfigs;

    @ConfigGroup
    public static class PushConfiguration extends BaseConfiguration {

        /**
         * Flow control threshold on queue level, each message queue will cache at most 1000 messages by default, Consider
         * the {@code pullBatchSize}, the instantaneous value may exceed the limit
         */
        @ConfigItem
        public Optional<Integer> pullThresholdForQueue;

        /**
         * Limit the cached message size on queue level, each message queue will cache at most 100 MiB messages by default,
         * Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
         *
         * <p>
         * The size of a message only measured by message body, so it's not accurate
         */
        @ConfigItem
        public Optional<Integer> pullThresholdSizeForQueue;

        /**
         * Concurrently max span offset.it has no effect on sequential consumption
         */
        @ConfigItem
        public Optional<Integer> consumeConcurrentlyMaxSpan;

        /**
         * Message pull Interval
         */
        @ConfigItem
        public Optional<Long> pullInterval;

        /**
         * Message model defines the way how messages are delivered to each consumer clients.
         * </p>
         *
         * RocketMQ supports two message models: clustering and broadcasting. If clustering is set, consumer clients with
         * the same {@link #consumerGroup} would only consume shards of the messages subscribed, which achieves load
         * balances; Conversely, if the broadcasting is set, each consumer client will consume all subscribed messages
         * separately.
         * </p>
         *
         * This field defaults to clustering.
         */
        @ConfigItem
        public Optional<MessageModel> messageModel;

        /**
         * Batch pull size
         */
        @ConfigItem
        public Optional<Integer> pullBatchSize;

        /**
         * Consuming point on consumer booting.
         * </p>
         *
         * There are three consuming points:
         * <ul>
         * <li>
         * <code>CONSUME_FROM_LAST_OFFSET</code>: consumer clients pick up where it stopped previously.
         * If it were a newly booting up consumer client, according aging of the consumer group, there are two
         * cases:
         * <ol>
         * <li>
         * if the consumer group is created so recently that the earliest message being subscribed has yet
         * expired, which means the consumer group represents a lately launched business, consuming will
         * start from the very beginning;
         * </li>
         * <li>
         * if the earliest message being subscribed has expired, consuming will start from the latest
         * messages, meaning messages born prior to the booting timestamp would be ignored.
         * </li>
         * </ol>
         * </li>
         * <li>
         * <code>CONSUME_FROM_FIRST_OFFSET</code>: Consumer client will start from earliest messages available.
         * </li>
         * <li>
         * <code>CONSUME_FROM_TIMESTAMP</code>: Consumer client will start from specified timestamp, which means
         * messages born prior to {@link #consumeTimestamp} will be ignored
         * </li>
         * </ul>
         */
        @ConfigItem
        public Optional<ConsumeFromWhere> consumeFromWhere;

        /**
         * topic
         */
        @ConfigItem
        public Optional<String> topic;

        /**
         * tag
         */
        @ConfigItem
        public Optional<String> tag;
    }
}
