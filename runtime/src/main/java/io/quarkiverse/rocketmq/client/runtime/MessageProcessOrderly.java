package io.quarkiverse.rocketmq.client.runtime;

import io.smallrye.mutiny.Uni;
import io.vertx.core.Future;
import io.vertx.core.impl.ContextInternal;
import io.vertx.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import io.vertx.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import io.vertx.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public abstract class MessageProcessOrderly implements MessageListenerOrderly {

    @Override
    public Future<ConsumeOrderlyStatus> consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
        ReturnType returnType = returnType();
        switch (returnType) {
            case FUTURE:
                return (Future<ConsumeOrderlyStatus>) process(msgs, context);
            case UNI:
                return ContextInternal.current().executeBlocking(blockPromise -> {
                    Uni<ConsumeOrderlyStatus> process = (Uni) process(msgs, context);
                    process.subscribe().with(blockPromise::complete, blockPromise::fail);
                });
            case OTHER:
                return ContextInternal.current().executeBlocking(blockPromise -> {
                    try {
                        blockPromise.complete((ConsumeOrderlyStatus) process(msgs, context));
                    } catch (Exception e) {
                        blockPromise.fail(e);
                    }
                });
        }
        return null;
    }

    protected abstract ReturnType returnType();

    protected abstract Object process(Object... args);
}
