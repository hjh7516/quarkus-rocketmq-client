package io.quarkiverse.rocketmq.client.runtime.config;

import java.util.Map;
import java.util.Optional;

import io.quarkus.runtime.annotations.ConfigGroup;
import io.quarkus.runtime.annotations.ConfigItem;
import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;

@ConfigRoot(phase = ConfigPhase.BUILD_AND_RUN_TIME_FIXED, name = PullConsumerConfig.CONSUMER_CONFIG_ROOT_NAME)
public class PullConsumerConfig {

    final static String CONSUMER_CONFIG_ROOT_NAME = "rocketmq.pull-consumer";

    /**
     * The default config.
     */
    @ConfigItem(name = ConfigItem.PARENT)
    public PullConfiguration defaultConfig;

    /**
     * Additional named config.
     */
    @ConfigItem(name = ConfigItem.PARENT)
    public Map<String, PullConfiguration> additionalConfigs;

    @ConfigGroup
    public static class PullConfiguration extends BaseConfiguration {

        /**
         * The flag for auto commit offset
         */
        @ConfigItem
        public Optional<Boolean> autoCommit;

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

        /**
         * Maximum commit offset interval time in milliseconds.
         */
        @ConfigItem
        public Optional<Long> autoCommitIntervalMillis;

        /**
         * Next auto commit deadline
         */
        @ConfigItem
        public Optional<Long> nextAutoCommitDeadline;
    }
}
