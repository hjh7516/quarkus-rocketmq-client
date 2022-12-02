package io.quarkiverse.rocketmq.client.runtime;

import io.quarkiverse.rocketmq.client.runtime.config.ProducerConfig.ProducerConfiguration;
import io.quarkiverse.rocketmq.client.runtime.config.PullConsumerConfig.PullConfiguration;
import io.quarkiverse.rocketmq.client.runtime.config.PushConsumerConfig.PushConfiguration;
import io.vertx.rocketmq.client.common.RocketmqOptions;

public class RocketmqClientUtil {
    public static final String DEFAULT_CLIENT = "<default>";

    public static boolean isDefault(String clientName) {
        return DEFAULT_CLIENT.equals(clientName);
    }

    public static RocketmqOptions buildOptions(PushConfiguration configuration) {

        RocketmqOptions options = new RocketmqOptions();
        options.setNamesrvAddr(configuration.namesrvAddr);
        configuration.groupName.ifPresent(options::setGroupName);
        configuration.namespace.ifPresent(options::setNamespace);
        configuration.unitName.ifPresent(options::setUnitName);
        configuration.pullThresholdForQueue.ifPresent(options::setPullThresholdForQueue);
        configuration.pullThresholdSizeForQueue.ifPresent(options::setPullThresholdSizeForQueue);
        configuration.consumeConcurrentlyMaxSpan.ifPresent(options::setConsumeConcurrentlyMaxSpan);
        configuration.pullInterval.ifPresent(options::setPullInterval);
        configuration.messageModel.ifPresent(options::setMessageModel);
        configuration.pullBatchSize.ifPresent(options::setPullBatchSize);
        configuration.consumeFromWhere.ifPresent(options::setConsumeFromWhere);
        options.setSubscriptionTopic(configuration.topic.orElse(null));
        configuration.tag.ifPresent(options::setSubscriptionTag);

        return options;
    }

    public static RocketmqOptions buildOptions(ProducerConfiguration configuration) {

        RocketmqOptions options = new RocketmqOptions();
        options.setNamesrvAddr(configuration.namesrvAddr);
        configuration.groupName.ifPresent(options::setGroupName);
        configuration.namespace.ifPresent(options::setNamespace);
        configuration.unitName.ifPresent(options::setUnitName);
        configuration.topicQueueNums.ifPresent(options::setTopicQueueNums);
        configuration.pollNameServerInterval.ifPresent(options::setPollNameServerInterval);
        configuration.heartbeatBrokerInterval.ifPresent(options::setHeartbeatBrokerInterval);
        configuration.permitsOneway.ifPresent(options::setPermitsOneway);

        return options;
    }

    public static RocketmqOptions buildOptions(PullConfiguration configuration) {

        RocketmqOptions options = new RocketmqOptions();
        options.setNamesrvAddr(configuration.namesrvAddr);
        configuration.groupName.ifPresent(options::setGroupName);
        configuration.namespace.ifPresent(options::setNamespace);
        configuration.unitName.ifPresent(options::setUnitName);
        configuration.autoCommit.ifPresent(options::setAutoCommit);
        configuration.autoCommitIntervalMillis.ifPresent(options::setAutoCommitIntervalMillis);
        configuration.nextAutoCommitDeadline.ifPresent(options::setNextAutoCommitDeadline);
        return options;
    }
}
