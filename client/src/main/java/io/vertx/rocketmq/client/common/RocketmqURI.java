package io.vertx.rocketmq.client.common;

import java.net.InetSocketAddress;

import org.apache.rocketmq.remoting.common.RemotingHelper;

import io.vertx.core.net.SocketAddress;

public class RocketmqURI {

    private final SocketAddress socketAddress;

    public RocketmqURI(String connectionString) {
        if (null == connectionString || connectionString.trim().length() == 0) {
            throw new IllegalArgumentException("Failed to parse the connection string");
        }
        socketAddress = SocketAddress
                .inetSocketAddress((InetSocketAddress) RemotingHelper.string2SocketAddress(connectionString));
    }

    public SocketAddress getSocketAddress() {
        return socketAddress;
    }
}
