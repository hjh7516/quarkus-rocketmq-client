/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.vertx.rocketmq.client.producer;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.NamespaceUtil;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.VertxInternal;
import io.vertx.rocketmq.client.common.RocketmqOptions;
import io.vertx.rocketmq.client.exception.MQClientException;

public class TransactionMQProducer extends DefaultMQProducer {
    private TransactionCheckListener transactionCheckListener;

    private TransactionListener transactionListener;

    public TransactionMQProducer(String producerGroup, VertxInternal vertx, RocketmqOptions options) {
        super(producerGroup, vertx, options);
    }

    @Override
    public void start() throws MQClientException {
        this.defaultMQProducerImpl.initTransactionEnv();
        super.start();
    }

    @Override
    public Future<Void> shutdown() {
        return super.shutdown().onSuccess(v -> this.defaultMQProducerImpl.destroyTransactionEnv());
    }

    @Override
    public Future<TransactionSendResult> sendMessageInTransaction(final Message msg, final Object arg) {
        Promise<TransactionSendResult> promise = Promise.promise();
        if (null == this.transactionListener) {
            promise.fail(new MQClientException("TransactionListener is null", null));
        } else {
            try {
                msg.setTopic(NamespaceUtil.wrapNamespace(this.getNamespace(), msg.getTopic()));
                this.defaultMQProducerImpl.sendMessageInTransaction(msg, null, arg)
                        .onFailure(promise::fail).onSuccess(promise::complete);
            } catch (Exception e) {
                promise.fail(e);
            }

        }

        return promise.future();
    }

    public TransactionCheckListener getTransactionCheckListener() {
        return transactionCheckListener;
    }

    /**
     * This method will be removed in the version 5.0.0 and set a custom thread pool is recommended.
     */
    @Deprecated
    public void setTransactionCheckListener(TransactionCheckListener transactionCheckListener) {
        this.transactionCheckListener = transactionCheckListener;
    }

    public TransactionListener getTransactionListener() {
        return transactionListener;
    }

    public void setTransactionListener(TransactionListener transactionListener) {
        this.transactionListener = transactionListener;
    }
}
