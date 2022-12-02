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
package io.vertx.rocketmq.client.trace.hook;

import java.util.ArrayList;

import org.apache.rocketmq.common.protocol.NamespaceUtil;

import io.vertx.rocketmq.client.hook.SendMessageContext;
import io.vertx.rocketmq.client.hook.SendMessageHook;
import io.vertx.rocketmq.client.producer.SendStatus;
import io.vertx.rocketmq.client.trace.AsyncTraceDispatcher;
import io.vertx.rocketmq.client.trace.TraceBean;
import io.vertx.rocketmq.client.trace.TraceContext;
import io.vertx.rocketmq.client.trace.TraceDispatcher;
import io.vertx.rocketmq.client.trace.TraceType;

public class SendMessageTraceHookImpl implements SendMessageHook {

    private final TraceDispatcher localDispatcher;

    public SendMessageTraceHookImpl(TraceDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
    }

    @Override
    public String hookName() {
        return "SendMessageTraceHook";
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {
        //if it is message trace data,then it doesn't recorded
        if (context == null
                || context.getMessage().getTopic().startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName())) {
            return;
        }
        //build the context content of TuxeTraceContext
        TraceContext tuxeContext = new TraceContext();
        tuxeContext.setTraceBeans(new ArrayList<>(1));
        context.setMqTraceContext(tuxeContext);
        tuxeContext.setTraceType(TraceType.Pub);
        tuxeContext.setGroupName(NamespaceUtil.withoutNamespace(context.getProducerGroup()));
        //build the data bean object of message trace
        TraceBean traceBean = new TraceBean();
        traceBean.setTopic(NamespaceUtil.withoutNamespace(context.getMessage().getTopic()));
        traceBean.setTags(context.getMessage().getTags());
        traceBean.setKeys(context.getMessage().getKeys());
        traceBean.setStoreHost(context.getBrokerAddr());
        traceBean.setBodyLength(context.getMessage().getBody().length);
        traceBean.setMsgType(context.getMsgType());
        tuxeContext.getTraceBeans().add(traceBean);
    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        //if it is message trace data,then it doesn't recorded
        if (context == null
                || context.getMessage().getTopic().startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName())
                || context.getMqTraceContext() == null) {
            return;
        }
        if (context.getSendResult() == null) {
            return;
        }

        if (context.getSendResult().getRegionId() == null
                || !context.getSendResult().isTraceOn()) {
            // if switch is false,skip it
            return;
        }

        TraceContext tuxeContext = (TraceContext) context.getMqTraceContext();
        TraceBean traceBean = tuxeContext.getTraceBeans().get(0);
        int costTime = (int) ((System.currentTimeMillis() - tuxeContext.getTimeStamp()) / tuxeContext.getTraceBeans().size());
        tuxeContext.setCostTime(costTime);
        tuxeContext.setSuccess(context.getSendResult().getSendStatus().equals(SendStatus.SEND_OK));
        tuxeContext.setRegionId(context.getSendResult().getRegionId());
        traceBean.setMsgId(context.getSendResult().getMsgId());
        traceBean.setOffsetMsgId(context.getSendResult().getOffsetMsgId());
        traceBean.setStoreTime(tuxeContext.getTimeStamp() + costTime / 2);
        localDispatcher.append(tuxeContext);
    }
}
