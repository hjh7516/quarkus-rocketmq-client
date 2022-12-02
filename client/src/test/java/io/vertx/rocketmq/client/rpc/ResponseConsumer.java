package io.vertx.rocketmq.client.rpc;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.future.CompositeFutureImpl;
import io.vertx.rocketmq.client.common.RocketmqOptions;
import io.vertx.rocketmq.client.consumer.DefaultMQPushConsumer;
import io.vertx.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import io.vertx.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import io.vertx.rocketmq.client.exception.MQClientException;
import io.vertx.rocketmq.client.producer.DefaultMQProducer;
import io.vertx.rocketmq.client.producer.SendResult;
import io.vertx.rocketmq.client.producer.TransactionMQProducer;
import io.vertx.rocketmq.client.utils.MessageUtil;

public class ResponseConsumer {

    public static void main(String[] args) throws Exception {

        RocketmqOptions options = new RocketmqOptions();
        options.setNamesrvAddr("192.168.19.132:9876");
        options.setGroupName("g-c-reactive-rpc");
        Vertx vertx = Vertx.vertx();
        vertx.exceptionHandler(Throwable::printStackTrace);
        String topic = "Reactive-RequestTopic";

        DefaultMQProducer replyProducer = new TransactionMQProducer(options.getGroupName(), (VertxInternal) vertx, options);
        replyProducer.setNamesrvAddr("192.168.19.132:9876");
        replyProducer.start();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(options.getGroupName(), (VertxInternal) vertx,
                options);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        // recommend client configs
        consumer.setPullTimeDelayMillsWhenException(0L);
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);

            List<Future<SendResult>> rs = new ArrayList<>();
            for (MessageExt msg : msgs) {
                try {
                    System.out.printf("handle message: %s", msg.toString());
                    String replyTo = MessageUtil.getReplyToClient(msg);
                    byte[] replyContent = "reply message contents.".getBytes();
                    Message replyMessage = MessageUtil.createReplyMessage(msg, replyContent);
                    Future<SendResult> resultFuture = replyProducer.send(replyMessage, 3000);
                    resultFuture.onComplete(ar -> {
                        if (ar.failed()) {
                            ar.cause().printStackTrace();
                        } else {
                            System.out.printf("reply to %s , %s %n", replyTo, ar.result().toString());
                        }
                    });

                } catch (MQClientException e) {
                    e.printStackTrace();
                }
            }

            Promise<ConsumeConcurrentlyStatus> promise = Promise.promise();
            CompositeFutureImpl.any(rs.toArray(new Future[0]))
                    .onFailure(promise::fail).onSuccess(v -> promise.complete(ConsumeConcurrentlyStatus.CONSUME_SUCCESS));
            return promise.future();
        });

        consumer.subscribe(topic, "*");
        consumer.start();
    }
}
