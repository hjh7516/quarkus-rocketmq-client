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
package io.vertx.rocketmq.client.impl.consumer;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;

import io.vertx.core.Future;
import io.vertx.core.impl.VertxInternal;
import io.vertx.rocketmq.client.impl.factory.MQClientInstance;
import io.vertx.rocketmq.client.log.ClientLogger;

public class PullMessageService extends ServiceThread {
    private final InternalLogger log = ClientLogger.getLog();
    private final LinkedBlockingQueue<PullRequest> pullRequestQueue = new LinkedBlockingQueue<>();
    private final MQClientInstance mQClientFactory;
    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(r -> new Thread(r, "PullMessageServiceScheduledThread"));
    private final VertxInternal vertx;

    public PullMessageService(MQClientInstance mQClientFactory, VertxInternal vertx) {
        this.mQClientFactory = mQClientFactory;
        this.vertx = vertx;
    }

    public void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(() -> PullMessageService.this.executePullRequestImmediately(pullRequest),
                    timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    public Future<Void> executePullRequestImmediately(final PullRequest pullRequest) {

        return vertx.executeBlocking(promise -> {
            try {
                this.pullRequestQueue.put(pullRequest);
            } catch (InterruptedException e) {
                log.error("executePullRequestImmediately pullRequestQueue.put", e);
            }
            promise.complete();
        });
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(r, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    private void pullMessage(final PullRequest pullRequest) {
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(pullRequest.getConsumerGroup());
        if (consumer != null) {
            DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
            impl.pullMessage(pullRequest);
        } else {
            log.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
        }
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                PullRequest pullRequest = this.pullRequestQueue.take();
                this.pullMessage(pullRequest);
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                log.error("Pull Message Service Run Method exception", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public void shutdown(boolean interrupt) {
        super.shutdown(interrupt);
        ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }

}
