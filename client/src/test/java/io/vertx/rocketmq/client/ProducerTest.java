package io.vertx.rocketmq.client;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.common.message.Message;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.future.CompositeFutureImpl;
import io.vertx.rocketmq.client.common.RocketmqOptions;
import io.vertx.rocketmq.client.exception.MQClientException;
import io.vertx.rocketmq.client.producer.DefaultMQProducer;
import io.vertx.rocketmq.client.producer.SendResult;
import io.vertx.rocketmq.client.producer.TransactionMQProducer;

public class ProducerTest {

    DefaultMQProducer producer;
    Vertx vertx;

    public static void main(String[] args) throws MQClientException {
        ProducerTest test = new ProducerTest();
        test.createProducer();
        test.test_sendSingle();
    }

    public void createProducer() throws MQClientException {
        RocketmqOptions options = new RocketmqOptions();
        options.setNamesrvAddr("192.168.19.132:9876");
        options.setGroupName("g-reactive-consumer");
        vertx = Vertx.vertx();
        vertx.exceptionHandler(Throwable::printStackTrace);
        producer = new TransactionMQProducer(options.getGroupName(), (VertxInternal) vertx, options);
        producer.start();
    }

    public void test_sendSingle() {
        String content = "reactive-test-" + 0;
        Message message = new Message("reactive-topic-0", content.getBytes(StandardCharsets.UTF_8));
        producer.send(message).onComplete(ar -> {
            if (ar.failed()) {
                ar.cause().printStackTrace();
            } else {
                System.out.println(ar.result());
            }
            //            producer.shutdown();
            //            vertx.close();
        });
    }

    public void test_send() {

        List<Future<SendResult>> rs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String content = "reactive-test-" + i;
            Message message = new Message("reactive-topic-2", content.getBytes(StandardCharsets.UTF_8));
            rs.add(producer.send(message));
        }
        CompositeFutureImpl.any(rs.toArray(new Future[0])).onComplete(ar -> {
            if (ar.failed()) {
                ar.cause().printStackTrace();
            } else {
                System.out.println(ar.result());
            }
            producer.shutdown();
            vertx.close();
        });
    }

    public void test_batch() {

        List<Message> msgs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String content = "reactive-topic-batch-" + i;
            Message message = new Message("reactive-topic-batch-3", content.getBytes(StandardCharsets.UTF_8));
            msgs.add(message);
        }

        producer.send(msgs).onComplete(ar -> {
            if (ar.failed()) {
                ar.cause().printStackTrace();
            } else {
                System.out.println(ar.result());
            }
            producer.shutdown();
            vertx.close();
        });
    }
}
