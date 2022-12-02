package io.quarkiverse.rocketmq.client.runtime.config;

import java.util.Map;
import java.util.Optional;

import io.quarkus.runtime.annotations.ConfigGroup;
import io.quarkus.runtime.annotations.ConfigItem;
import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;

@ConfigRoot(phase = ConfigPhase.BUILD_AND_RUN_TIME_FIXED, name = ProducerConfig.PRODUCER_CONFIG_ROOT_NAME)
public class ProducerConfig {
    final static String PRODUCER_CONFIG_ROOT_NAME = "rocketmq.producer";

    /**
     * The default config.
     */
    @ConfigItem(name = ConfigItem.PARENT)
    public ProducerConfiguration defaultConfig;

    /**
     * Additional named config.
     */
    @ConfigItem(name = ConfigItem.PARENT)
    public Map<String, ProducerConfiguration> additionalConfigs;

    @ConfigGroup
    public static class ProducerConfiguration extends BaseConfiguration {

        /**
         * Number of queues to create per default topic.
         */
        @ConfigItem(defaultValueDocumentation = "4")
        public Optional<Integer> topicQueueNums;

        /**
         * Pulling topic information interval from the named server
         */
        @ConfigItem(defaultValueDocumentation = "30_000")
        public Optional<Long> pollNameServerInterval;

        /**
         * Heartbeat interval in microseconds with message broker
         */
        @ConfigItem(defaultValueDocumentation = "30_000")
        public Optional<Long> heartbeatBrokerInterval;

        /**
         * permitsOneway
         */
        @ConfigItem(defaultValueDocumentation = "65535")
        public Optional<Integer> permitsOneway;
    }
}
