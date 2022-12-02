package io.quarkiverse.rocketmq.client.deployment;

import org.jboss.jandex.DotName;

public final class RocketmqDotNames {

    public static final DotName PROCESS_CONCURRENTLY = DotName
            .createSimple("io.quarkiverse.rocketmq.client.runtime.MessageProcessConcurrently");
    public static final DotName PROCESS_ORDERLY = DotName.createSimple("io.quarkiverse.rocketmq.client.runtime.MessageProcessOrderly");
    public static final DotName INCOMING = DotName.createSimple("io.quarkiverse.rocketmq.client.runtime.RocketmqIncoming");
    public static final DotName RETURN_FUTURE = DotName.createSimple("io.vertx.core.Future");
    public static final DotName RETURN_UNI = DotName.createSimple("io.smallrye.mutiny.Uni");

    public static final DotName ROCKETMQ_INCOMING = DotName.createSimple("io.quarkiverse.rocketmq.client.runtime.RocketmqIncoming");
    public static final DotName LIST = DotName.createSimple("java.util.List");
    public static final DotName CONCURRENTLY_CONTEXT = DotName
            .createSimple("io.vertx.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext");
    public static final DotName ORDERLY_CONTEXT = DotName
            .createSimple("io.vertx.rocketmq.client.consumer.listener.ConsumeOrderlyContext");
    public static final DotName MESSAGE = DotName.createSimple("org.apache.rocketmq.common.message.MessageExt");
    public static final DotName CONCURRENTLY_STATUS = DotName
            .createSimple("io.vertx.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus");
    public static final DotName ORDERLY_STATUS = DotName
            .createSimple("io.vertx.rocketmq.client.consumer.listener.ConsumeOrderlyStatus");

}
