package io.vertx.rocketmq.client.connection;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyDecoder;
import org.apache.rocketmq.remoting.netty.NettyEncoder;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingCommandType;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.core.net.impl.NetSocketInternal;
import io.vertx.core.net.impl.pool.Lease;
import io.vertx.rocketmq.client.impl.ClientRemotingProcessor;
import io.vertx.rocketmq.client.producer.consts.CommunicationMode;

public class RocketmqConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    private static final Handler<Throwable> DEFAULT_EXCEPTION_HANDLER = t -> log.error("Unhandled Error", t);
    private final NetSocketInternal netSocket;
    private final VertxInternal vertx;
    private final ContextInternal context;
    private Runnable onEvict;
    private volatile boolean isValid = true;
    private Handler<Throwable> onException;
    private ClientRemotingProcessor requestProcessor;
    private final Map<Integer, PromiseInternal<RemotingCommand>> responseTable = new ConcurrentHashMap<>(256);
    private Lease<RocketmqConnection> lease;

    public RocketmqConnection(NetSocketInternal netSocket, VertxInternal vertx, ContextInternal context) {
        this.netSocket = netSocket;
        this.vertx = vertx;
        this.context = context;
    }

    public void setLease(Lease<RocketmqConnection> lease) {
        this.lease = lease;
    }

    public void init() {
        netSocket.channelHandlerContext()
                .pipeline()
                .addBefore("handler", "encoder", new NettyEncoder())
                .addBefore("handler", "decoder", new NettyDecoder())
                .addBefore("handler", "client", new NettyClientHandler());
        netSocket.messageHandler(msg -> {
            try {
                handleMessage(msg);
            } catch (Exception e) {
                fail(e);
            }
        });
    }

    public Future<RemotingCommand> send(RemotingCommand cmd, CommunicationMode communicationMode) {
        return send(cmd, communicationMode, null);
    }

    public Future<RemotingCommand> send(RemotingCommand cmd, CommunicationMode communicationMode, Long timeoutMillis) {
        PromiseInternal<RemotingCommand> promise = vertx.promise();
        context.execute(v -> netSocket.writeMessage(cmd, write -> {
            System.out.println();
            lease.recycle();
            if (write.failed()) {
                promise.fail(write.cause());
                responseTable.remove(cmd.getOpaque());
            } else {
                if (!CommunicationMode.SYNC.equals(communicationMode)) {
                    promise.complete();
                } else {
                    responseTable.put(cmd.getOpaque(), promise);
                }
            }

        }));

        if (!Objects.isNull(timeoutMillis) && timeoutMillis > 0) {
            vertx.setTimer(timeoutMillis, id -> {
                PromiseInternal<RemotingCommand> promiseInternal = responseTable.get(cmd.getOpaque());
                if (!Objects.isNull(promiseInternal) && !promiseInternal.isComplete()) {

                    promiseInternal.fail(new RemotingTimeoutException("invokeSync call timeout"));
                    responseTable.remove(cmd.getOpaque());
                }
            });
        }
        return promise.future();
    }

    public void handleMessage(Object msg) {
        if (msg instanceof RemotingCommandContext) {
            RemotingCommandContext cmdContext = (RemotingCommandContext) msg;
            RemotingCommand cmd = cmdContext.getRemotingCmd();
            if (cmd.getType().equals(RemotingCommandType.REQUEST_COMMAND)) {
                requestProcessor.processRequest(cmdContext.getCtx(), cmd).onFailure(e -> {
                    log.error("process request exception", e);
                    log.error(cmd.toString());

                    if (!cmd.isOnewayRPC()) {
                        final RemotingCommand response = RemotingCommand.createResponseCommand(
                                RemotingSysResponseCode.SYSTEM_ERROR,
                                RemotingHelper.exceptionSimpleDesc(e));
                        response.setOpaque(cmd.getOpaque());
                        cmdContext.getCtx().writeAndFlush(response);
                    }
                }).onSuccess(response -> {
                    if (!cmd.isOnewayRPC()) {
                        if (response != null) {
                            response.setOpaque(cmd.getOpaque());
                            response.markResponseType();
                            try {
                                cmdContext.getCtx().writeAndFlush(response);
                            } catch (Throwable e) {
                                log.error("process request over, but response failed", e);
                                log.error(cmd.toString());
                                log.error(response.toString());
                            }
                        }
                    }
                });
            } else {
                PromiseInternal<RemotingCommand> promise = responseTable.get(cmd.getOpaque());
                if (!Objects.isNull(promise) && !promise.isComplete()) {
                    responseTable.remove(cmd.getOpaque());
                    context.execute(Future.succeededFuture(cmd), f -> promise.complete(f.result()));
                }
            }

        }
    }

    public RocketmqConnection exceptionHandler(Handler<Throwable> handler) {
        this.onException = handler;
        return this;
    }

    public void requestProcessor(ClientRemotingProcessor requestProcessor) {
        this.requestProcessor = requestProcessor;
    }

    RocketmqConnection evictHandler(Runnable handler) {
        this.onEvict = handler;
        return this;
    }

    public void fatal(Throwable t) {
        cleanResponseTable(t).onComplete(v -> log.error("socket error", t));
    }

    public void fail(Throwable t) {
        Handler<Throwable> exceptionHandler = onException == null ? DEFAULT_EXCEPTION_HANDLER : onException;
        context.execute(t, exceptionHandler);
    }

    public void end(Void v) {
        context.execute(arg -> {
            netSocket.close();
            isValid = false;
        });
    }

    public boolean isValid() {
        return isValid;
    }

    private Future<Void> cleanResponseTable(Throwable t) {
        isValid = false;
        final PromiseInternal<Void> promise = vertx.promise();
        context.execute(v -> {
            if (!Objects.isNull(onEvict)) {
                onEvict.run();
            }
            if (responseTable.isEmpty()) {
                promise.complete();
            } else {
                Iterator<Map.Entry<Integer, PromiseInternal<RemotingCommand>>> iterator = responseTable.entrySet().iterator();
                while (iterator.hasNext()) {
                    iterator.next().getValue().fail(t);
                    iterator.remove();
                }

                promise.complete();
            }
        });

        return promise.future();
    }

    static class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) {
            ctx.fireChannelRead(new RemotingCommandContext(ctx, msg));
        }
    }

    static class RemotingCommandContext {
        private final ChannelHandlerContext ctx;
        private final RemotingCommand remotingCmd;

        RemotingCommandContext(ChannelHandlerContext ctx, RemotingCommand remotingCmd) {
            this.ctx = ctx;
            this.remotingCmd = remotingCmd;
        }

        public ChannelHandlerContext getCtx() {
            return ctx;
        }

        public RemotingCommand getRemotingCmd() {
            return remotingCmd;
        }
    }
}
