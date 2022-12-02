package io.quarkiverse.rocketmq.client.runtime;

import io.smallrye.mutiny.Uni;
import io.vertx.core.Future;
import io.vertx.core.impl.ContextInternal;
import io.vertx.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import io.vertx.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import io.vertx.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public abstract class MessageProcessConcurrently implements MessageListenerConcurrently {

    @Override
    public Future<ConsumeConcurrentlyStatus> consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        ReturnType returnType = returnType();
        switch (returnType) {
            case FUTURE:
                return (Future<ConsumeConcurrentlyStatus>) process(msgs, context);
            case UNI:
                return ContextInternal.current().executeBlocking(blockPromise -> {
                    Uni<ConsumeConcurrentlyStatus> process = (Uni) process(msgs, context);
                    process.subscribe().with(blockPromise::complete, blockPromise::fail);
                });
            case OTHER:
                return ContextInternal.current().executeBlocking(blockPromise -> {
                    try {
                        blockPromise.complete((ConsumeConcurrentlyStatus) process(msgs, context));
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
