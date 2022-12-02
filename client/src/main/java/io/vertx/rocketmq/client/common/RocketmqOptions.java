package io.vertx.rocketmq.client.common;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.utils.NameServerAddressUtils;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

import io.vertx.core.net.NetClientOptions;
import io.vertx.rocketmq.client.ClientConfig;

public class RocketmqOptions {

    private String poolName;
    private int maxPoolSize;
    private NetClientOptions netClientOptions;
    private String namesrvAddr;
    private int topicQueueNums = 4;
    private long pollNameServerInterval = 1000 * 30;
    private String unitName;
    private long heartbeatBrokerInterval = 1000 * 30;
    private int permitsOneway = 65535;
    private int persistConsumerOffsetInterval = 1000 * 5;
    private boolean vipChannelEnabled = Boolean
            .parseBoolean(System.getProperty(ClientConfig.SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "false"));
    private String namespace;
    private LanguageCode language = LanguageCode.JAVA;
    private String clientIP = RemotingUtil.getLocalAddress();
    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    private String groupName;

    private Integer pullThresholdForQueue = 1000;
    private Integer pullThresholdSizeForQueue = 100;
    private Integer consumeConcurrentlyMaxSpan = 2000;
    private Long pullInterval = 0L;
    private MessageModel messageModel = MessageModel.CLUSTERING;
    private Integer pullBatchSize = 32;
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    private String subscriptionTopic;
    private String subscriptionTag = "*";

    private Boolean autoCommit = true;
    private Long autoCommitIntervalMillis = 5 * 1000L;
    private Long nextAutoCommitDeadline = -1L;

    public Boolean getAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(Boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public Long getAutoCommitIntervalMillis() {
        return autoCommitIntervalMillis;
    }

    public void setAutoCommitIntervalMillis(Long autoCommitIntervalMillis) {
        this.autoCommitIntervalMillis = autoCommitIntervalMillis;
    }

    public Long getNextAutoCommitDeadline() {
        return nextAutoCommitDeadline;
    }

    public void setNextAutoCommitDeadline(Long nextAutoCommitDeadline) {
        this.nextAutoCommitDeadline = nextAutoCommitDeadline;
    }

    public String getSubscriptionTopic() {
        return subscriptionTopic;
    }

    public void setSubscriptionTopic(String subscriptionTopic) {
        this.subscriptionTopic = subscriptionTopic;
    }

    public String getSubscriptionTag() {
        return subscriptionTag;
    }

    public void setSubscriptionTag(String subscriptionTag) {
        this.subscriptionTag = subscriptionTag;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public Integer getPullThresholdForQueue() {
        return pullThresholdForQueue;
    }

    public void setPullThresholdForQueue(Integer pullThresholdForQueue) {
        this.pullThresholdForQueue = pullThresholdForQueue;
    }

    public Integer getPullThresholdSizeForQueue() {
        return pullThresholdSizeForQueue;
    }

    public void setPullThresholdSizeForQueue(Integer pullThresholdSizeForQueue) {
        this.pullThresholdSizeForQueue = pullThresholdSizeForQueue;
    }

    public Integer getConsumeConcurrentlyMaxSpan() {
        return consumeConcurrentlyMaxSpan;
    }

    public void setConsumeConcurrentlyMaxSpan(Integer consumeConcurrentlyMaxSpan) {
        this.consumeConcurrentlyMaxSpan = consumeConcurrentlyMaxSpan;
    }

    public Long getPullInterval() {
        return pullInterval;
    }

    public void setPullInterval(Long pullInterval) {
        this.pullInterval = pullInterval;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public Integer getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(Integer pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getClientIP() {
        return clientIP;
    }

    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public boolean isVipChannelEnabled() {
        return vipChannelEnabled;
    }

    public void setVipChannelEnabled(boolean vipChannelEnabled) {
        this.vipChannelEnabled = vipChannelEnabled;
    }

    public int getPersistConsumerOffsetInterval() {
        return persistConsumerOffsetInterval;
    }

    public void setPersistConsumerOffsetInterval(int persistConsumerOffsetInterval) {
        this.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
    }

    public RocketmqOptions() {
        this.poolName = UUID.randomUUID().toString();
        this.maxPoolSize = 6;
        this.netClientOptions = new NetClientOptions();
    }

    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public void setNetClientOptions(NetClientOptions netClientOptions) {
        this.netClientOptions = netClientOptions;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public void setTopicQueueNums(int topicQueueNums) {
        this.topicQueueNums = topicQueueNums;
    }

    public void setPollNameServerInterval(long pollNameServerInterval) {
        this.pollNameServerInterval = pollNameServerInterval;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public void setHeartbeatBrokerInterval(long heartbeatBrokerInterval) {
        this.heartbeatBrokerInterval = heartbeatBrokerInterval;
    }

    public void setPermitsOneway(int permitsOneway) {
        this.permitsOneway = permitsOneway;
    }

    public NetClientOptions getNetClientOptions() {
        return netClientOptions;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public String getPoolName() {
        return poolName;
    }

    public int getTopicQueueNums() {
        return topicQueueNums;
    }

    public long getPollNameServerInterval() {
        return pollNameServerInterval;
    }

    public int getPermitsOneway() {
        return permitsOneway;
    }

    public String getNamesrvAddr() {
        if (StringUtils.isNotEmpty(namesrvAddr)) {
            return namesrvAddr;
        }
        namesrvAddr = NameServerAddressUtils.getNameServerAddresses();
        if (StringUtils.isNotEmpty(namesrvAddr)
                && NameServerAddressUtils.NAMESRV_ENDPOINT_PATTERN.matcher(namesrvAddr.trim()).matches()) {
            return namesrvAddr.substring(NameServerAddressUtils.ENDPOINT_PREFIX.length());
        }

        return namesrvAddr;
    }

    public String withNamespace(String resource) {
        return NamespaceUtil.wrapNamespace(this.getNamespace(), resource);
    }

    public Set<String> withNamespace(Set<String> resourceSet) {
        Set<String> resourceWithNamespace = new HashSet<>();
        for (String resource : resourceSet) {
            resourceWithNamespace.add(withNamespace(resource));
        }
        return resourceWithNamespace;
    }

    public String withoutNamespace(String resource) {
        return NamespaceUtil.withoutNamespace(resource, this.getNamespace());
    }

    public Set<String> withoutNamespace(Set<String> resourceSet) {
        Set<String> resourceWithoutNamespace = new HashSet<>();
        for (String resource : resourceSet) {
            resourceWithoutNamespace.add(withoutNamespace(resource));
        }
        return resourceWithoutNamespace;
    }

    public MessageQueue queueWithNamespace(MessageQueue queue) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queue;
        }
        return new MessageQueue(withNamespace(queue.getTopic()), queue.getBrokerName(), queue.getQueueId());
    }

    public Collection<MessageQueue> queuesWithNamespace(Collection<MessageQueue> queues) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queues;
        }
        for (MessageQueue queue : queues) {
            queue.setTopic(withNamespace(queue.getTopic()));
        }
        return queues;
    }

    public String buildMQClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClientIP());

        sb.append("@");
        sb.append(this.getInstanceName());
        if (!UtilAll.isBlank(this.unitName)) {
            sb.append("@");
            sb.append(this.unitName);
        }

        return sb.toString();
    }

    public String getUnitName() {
        return unitName;
    }

    public long getHeartbeatBrokerInterval() {
        return heartbeatBrokerInterval;
    }

    public void changeInstanceNameToPID() {
        if (this.instanceName.equals("DEFAULT")) {
            this.instanceName = String.valueOf(UtilAll.getPid());
        }
    }

    public RocketmqOptions copyWithConditon(String groupName) {
        RocketmqOptions options = new RocketmqOptions();
        options.setGroupName(groupName);
        return options;
    }
}
