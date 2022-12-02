package io.vertx.rocketmq.client.connection;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.EventLoopContext;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.impl.pool.ConnectionManager;
import io.vertx.core.net.impl.pool.ConnectionPool;
import io.vertx.core.net.impl.pool.Endpoint;
import io.vertx.core.net.impl.pool.Lease;
import io.vertx.core.spi.metrics.PoolMetrics;
import io.vertx.core.spi.metrics.VertxMetrics;
import io.vertx.rocketmq.client.common.RocketmqOptions;
import io.vertx.rocketmq.client.common.TopAddress;
import io.vertx.rocketmq.client.exception.MQClientException;
import io.vertx.rocketmq.client.impl.ClientRemotingProcessor;
import io.vertx.rocketmq.client.impl.factory.MQClientInstance;

public class RocketmqConnectionManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
    private final VertxInternal vertx;
    private final PoolMetrics metrics;
    private final NetClient netClient;
    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<>();
    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<>();
    private final TopAddress topAddress;
    private String nameSrvAddr = null;
    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());
    private final MQClientInstance mqClientFactory;

    private final ConnectionManager<String, Lease<RocketmqConnection>> connectionManager = new ConnectionManager<>(
            this::create);

    public RocketmqConnectionManager(VertxInternal vertx, RocketmqOptions options, MQClientInstance mqClientFactory) {
        this.vertx = vertx;
        VertxMetrics metricsSPI = this.vertx.metricsSPI();
        metrics = metricsSPI != null ? metricsSPI.createPoolMetrics("rocketmq", options.getPoolName(), options.getMaxPoolSize())
                : null;
        netClient = vertx.createNetClient(options.getNetClientOptions());
        topAddress = new TopAddress(vertx, MixAll.getWSAddr(), options.getUnitName());
        this.mqClientFactory = mqClientFactory;
    }

    private Endpoint<Lease<RocketmqConnection>> create(String connectionString, ContextInternal ctx, Runnable dispose) {
        return new RocketmqEndpoint(netClient, connectionString, dispose);
    }

    public Future<RocketmqConnection> getConnection(String connectionString) {
        final PromiseInternal<Lease<RocketmqConnection>> promise = vertx.promise();
        final ContextInternal ctx = promise.context();
        final EventLoopContext eventLoopContext;
        if (ctx instanceof EventLoopContext) {
            eventLoopContext = (EventLoopContext) ctx;
        } else {
            eventLoopContext = vertx.createEventLoopContext();
        }

        final boolean metricsEnabled = metrics != null;
        final Object queueMetric = metricsEnabled ? metrics.submitted() : null;
        if (Objects.isNull(connectionString)) {
            connectionString = getNameserverAddr();
        }
        if (Objects.isNull(connectionString)) {
            promise.fail(new MQClientException(-1, "connection addr is null"));
            return promise.future().map(Lease::get);
        }

        //        String finalConnectionString = connectionString;
        //        Runnable dispose = () -> endpointMap.remove(finalConnectionString);
        //        Endpoint<RocketmqConnection> endpoint = endpointMap.computeIfAbsent(connectionString,
        //                connStr -> new RocketmqEndpoint(connectionProvider, connStr, dispose));
        //        endpoint.getConnection(eventLoopContext, 0L, promise);

        connectionManager.getConnection(eventLoopContext, connectionString, promise);
        return promise.future().onFailure(err -> {
            if (metricsEnabled) {
                metrics.rejected(queueMetric);
            }
        }).map(lease -> {
            if (metricsEnabled) {
                metrics.begin(queueMetric);
            }
            lease.get().setLease(lease);
            return lease.get();
        });
    }

    public Future<String> fetchNameServerAddr() {
        PromiseInternal<String> promise = vertx.promise();
        Future<String> fetchNSAddr = topAddress.fetchNSAddr(3000);
        fetchNSAddr.onFailure(promise::fail).onSuccess(addrs -> {
            if (addrs != null) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("name server address changed, old=" + this.nameSrvAddr + ", new=" + addrs);
                    String[] addrArray = addrs.split(";");
                    List<String> list = Arrays.asList(addrArray);
                    this.updateNameServerAddressList(list);
                    this.nameSrvAddr = addrs;
                    promise.complete(nameSrvAddr);
                }
            }
        });

        return promise.future();
    }

    public void updateNameServerAddressList(List<String> addrs) {
        List<String> old = this.namesrvAddrList.get();
        boolean update = false;
        if (Objects.isNull(addrs) || addrs.isEmpty()) {
            return;
        }
        if (null == old) {
            update = true;
        } else if (addrs.size() != old.size()) {
            update = true;
        } else {
            for (String addr : addrs) {
                if (!old.contains(addr)) {
                    update = true;
                    break;
                }
            }
        }
        if (!update) {
            return;
        }
        Collections.shuffle(addrs);
        log.info("name server address updated. NEW : {} , OLD: {}", addrs, old);
        this.namesrvAddrList.set(addrs);
        if (!addrs.contains(this.namesrvAddrChoosed.get())) {
            this.namesrvAddrChoosed.set(null);
        }
    }

    public Future<Void> shutdown() {
        if (null != metrics) {
            metrics.close();
        }
        connectionManager.close();
        return netClient.close();
    }

    private String getNameserverAddr() {
        String addr = this.namesrvAddrChoosed.get();
        if (!Objects.isNull(addr)) {
            return addr;
        }

        List<String> addrList = this.namesrvAddrList.get();
        if (addrList != null && !addrList.isEmpty()) {
            for (int i = 0; i < addrList.size(); i++) {
                int index = this.namesrvIndex.incrementAndGet();
                index = Math.abs(index);
                index = index % addrList.size();
                String newAddr = addrList.get(index);

                this.namesrvAddrChoosed.set(newAddr);
                log.info("new name server is chosen. OLD: {} , NEW: {}. namesrvIndex = {}", addr, newAddr, namesrvIndex);
            }
        }

        return this.namesrvAddrChoosed.get();
    }

    private static int initValueIndex() {
        Random r = new Random();
        return Math.abs(r.nextInt() % 999) % 999;
    }

    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }

    class RocketmqEndpoint extends Endpoint<Lease<RocketmqConnection>> {

        private final ConnectionPool<RocketmqConnection> pool;

        public RocketmqEndpoint(NetClient netClient, String connectionString, Runnable dispose) {
            super(dispose);
            RocketmqConnectionProvider connectionProvider = new RocketmqConnectionProvider(netClient, connectionString);
            pool = ConnectionPool.pool(connectionProvider, new int[] { 1 });
        }

        @Override
        public void requestConnection(ContextInternal ctx, long timeout,
                Handler<AsyncResult<Lease<RocketmqConnection>>> handler) {
            pool.acquire(ctx, 0, ar -> {
                if (ar.succeeded()) {
                    RocketmqConnection connection = ar.result().get();
                    connection.evictHandler(this::decRefCount);
                    connection.requestProcessor(new ClientRemotingProcessor(mqClientFactory));
                    handler.handle(ar);
                }
            });
        }
    }
}
