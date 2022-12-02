package io.vertx.rocketmq.client;

import io.smallrye.mutiny.vertx.AsyncResultUni;
import io.vertx.core.Future;

public class ReactiveUtil {

    public static <T> T waitOne(Future<T> future) {
        Object indefinitely = AsyncResultUni.toUni(handler -> future.onComplete(ar -> {
            if (ar.succeeded()) {
                handler.handle(Future.succeededFuture(ar.result()));
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        })).await().indefinitely();
        return (T) indefinitely;
    }
}
