package io.quarkiverse.rocketmq.client.deployment.items;

import io.quarkus.builder.item.MultiBuildItem;

public final class ConsumerBuilItem extends MultiBuildItem {

    private final String identifier;

    private final String configKey;

    private final String implClz;

    public ConsumerBuilItem(String identifier, String configKey, String implClz) {
        this.identifier = identifier;
        this.configKey = configKey;
        this.implClz = implClz;
    }

    public String getConfigKey() {
        return configKey;
    }

    public String getIdentifier() {
        return identifier;
    }

    public String getImplClz() {
        return implClz;
    }
}
