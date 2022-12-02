package io.vertx.rocketmq.client;

import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import io.vertx.rocketmq.client.common.RocketmqOptions;
import io.vertx.rocketmq.client.consumer.DefaultMQPushConsumer;
import io.vertx.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import io.vertx.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import io.vertx.rocketmq.client.exception.MQClientException;

public class ConsumerTest {

    public static void main(String[] args) throws MQClientException {
        RocketmqOptions options = new RocketmqOptions();
        options.setNamesrvAddr("192.168.19.132:9876");
        options.setGroupName("g-reactive-test");
        Vertx vertx = Vertx.vertx();
        vertx.exceptionHandler(Throwable::printStackTrace);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(options.getGroupName(), (VertxInternal) vertx,
                options);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe("reactive-mq", "*");
        consumer.setMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.println("content : " + new String(msg.getBody()));
            }
            Promise<ConsumeConcurrentlyStatus> promise = Promise.promise();
            promise.complete(ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
            return promise.future();
        });
        consumer.start();
    }
}
