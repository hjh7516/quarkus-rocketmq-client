package io.vertx.rocketmq.client.connection;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.impl.EventLoopContext;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.impl.NetSocketInternal;
import io.vertx.core.net.impl.pool.ConnectResult;
import io.vertx.core.net.impl.pool.PoolConnector;
import io.vertx.rocketmq.client.common.RocketmqURI;

public class RocketmqConnectionProvider implements PoolConnector<RocketmqConnection> {

    private final NetClient netClient;
    private final String connectionString;

    public RocketmqConnectionProvider(NetClient netClient, String connectionString) {
        this.netClient = netClient;
        this.connectionString = connectionString;
    }

    public Future<Void> shutdown() {
        Promise<Void> promise = Promise.promise();
        if (null != this.netClient) {
            this.netClient.close(promise);
        } else {
            promise.complete();
        }
        return promise.future();
    }

    @Override
    public void connect(EventLoopContext context, Listener listener,
            Handler<AsyncResult<ConnectResult<RocketmqConnection>>> handler) {
        RocketmqURI uri = new RocketmqURI(connectionString);
        netClient.connect(uri.getSocketAddress(), clientConnect -> {
            if (clientConnect.failed()) {
                context.emit(Future.failedFuture(clientConnect.cause()), handler);
            } else {
                NetSocketInternal netSocket = (NetSocketInternal) clientConnect.result();
                RocketmqConnection newConn = new RocketmqConnection(netSocket, context.owner(), context);
                newConn.init();
                netSocket.closeHandler(newConn::end)
                        .exceptionHandler(newConn::fatal);
                context.emit(Future.succeededFuture(new ConnectResult<>(newConn, 1, 0)), handler);
            }
        });
    }

    @Override
    public boolean isValid(RocketmqConnection connection) {
        return connection.isValid();
    }
}
