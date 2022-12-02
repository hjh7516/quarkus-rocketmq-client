package io.quarkiverse.rocketmq.client.runtime.config;

import java.util.Optional;

import io.quarkus.runtime.annotations.ConfigItem;

public abstract class BaseConfiguration {

    /**
     * Name Server address
     */
    @ConfigItem(defaultValue = "localhost:9876", name = "namesrvAddr")
    public String namesrvAddr;

    /**
     * Gropup name
     */
    @ConfigItem(defaultValue = "DEFAULT_GROUP", name = "groupName")
    public Optional<String> groupName;

    /**
     * namespace
     */
    @ConfigItem(name = "namespace")
    public Optional<String> namespace;

    /**
     * Used to build the client ID
     */
    @ConfigItem(name = "unitName")
    public Optional<String> unitName;
}
