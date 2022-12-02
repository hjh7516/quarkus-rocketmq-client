package io.vertx.rocketmq.client;

import org.apache.rocketmq.logging.InternalLogger;

import io.vertx.rocketmq.client.log.ClientLogger;

public class LogTest {

    public static void main(String[] args) {
        InternalLogger log = ClientLogger.getLog();
        log.warn("test");
    }
}
