package io.quarkiverse.rocketmq.client.deployment;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import io.vertx.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class MessageProcess {

    public Future<ConsumeConcurrentlyStatus> process(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        Promise<ConsumeConcurrentlyStatus> promise = Promise.promise();
        System.out.println("消息");
        promise.complete(ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        return promise.future();
    }
}
