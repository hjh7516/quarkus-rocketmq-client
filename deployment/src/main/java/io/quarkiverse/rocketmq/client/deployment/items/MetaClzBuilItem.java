package io.quarkiverse.rocketmq.client.deployment.items;

import org.jboss.jandex.MethodInfo;

import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.builder.item.MultiBuildItem;

public final class MetaClzBuilItem extends MultiBuildItem {

    private final BeanInfo bean;

    private final MethodInfo method;

    public MetaClzBuilItem(BeanInfo bean, MethodInfo method) {
        this.bean = bean;
        this.method = method;
    }

    public BeanInfo getBean() {
        return bean;
    }

    public MethodInfo getMethod() {
        return method;
    }
}
