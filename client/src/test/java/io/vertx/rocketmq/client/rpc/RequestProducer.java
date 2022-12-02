package io.vertx.rocketmq.client.rpc;

import java.util.Date;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import io.vertx.rocketmq.client.common.RocketmqOptions;
import io.vertx.rocketmq.client.exception.MQClientException;
import io.vertx.rocketmq.client.producer.DefaultMQProducer;
import io.vertx.rocketmq.client.producer.TransactionMQProducer;

public class RequestProducer {

    DefaultMQProducer producer;
    Vertx vertx;

    public static void main(String[] args) throws Exception {

        RequestProducer requestProducer = new RequestProducer();
        requestProducer.createProducer();
        requestProducer.request();
    }

    public void createProducer() throws MQClientException {
        RocketmqOptions options = new RocketmqOptions();
        options.setNamesrvAddr("192.168.19.132:9876");
        options.setGroupName("g-p-reactive-rpc");
        vertx = Vertx.vertx();
        vertx.exceptionHandler(Throwable::printStackTrace);
        producer = new TransactionMQProducer(options.getGroupName(), (VertxInternal) vertx, options);
        producer.start();
        System.out.println(DateFormatUtils.format(new Date(), "HH:mm:ss") + ", Start producer finish!!!");
    }

    public void request() throws Exception {
        String topic = "Reactive-RequestTopic";
        long ttl = 3000;

        Message msg = new Message(topic,
                "",
                "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));

        long begin = System.currentTimeMillis();
        producer.request(msg, ttl).onComplete(ar -> {
            if (ar.failed()) {
                ar.cause().printStackTrace();
            } else {
                long cost = System.currentTimeMillis() - begin;
                System.out.printf("request to <%s> cost: %d replyMessage: %s %n", topic, cost,
                        new String(ar.result().getBody()));
            }
        });

    }
}
