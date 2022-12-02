package io.quarkiverse.rocketmq.client.deployment.component;

import io.quarkiverse.rocketmq.client.deployment.DeploymentConfigException;
import io.quarkiverse.rocketmq.client.deployment.RocketmqDotNames;
import io.quarkiverse.rocketmq.client.deployment.items.MetaClzBuilItem;
import io.quarkiverse.rocketmq.client.runtime.config.PushConsumerConfig;
import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.deployment.annotations.BuildProducer;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.Type;

import java.util.List;
import java.util.Objects;

import static io.quarkiverse.rocketmq.client.deployment.RocketmqDotNames.CONCURRENTLY_STATUS;
import static io.quarkiverse.rocketmq.client.deployment.RocketmqDotNames.ORDERLY_STATUS;
import static io.quarkiverse.rocketmq.client.deployment.RocketmqDotNames.RETURN_FUTURE;
import static io.quarkiverse.rocketmq.client.deployment.RocketmqDotNames.RETURN_UNI;

public class BeanMethodValidator {

    private MethodInfo method;

    private PushConsumerConfig pushConsumerConfig;

    private BeanInfo bean;

    private boolean isOrderly;

    private boolean allow = Boolean.TRUE;

    public BeanMethodValidator(PushConsumerConfig pushConsumerConfig) {
        this.pushConsumerConfig = pushConsumerConfig;
    }

    public BeanMethodValidator from(BeanInfo bean, MethodInfo method) {
        this.method = method;
        this.bean = bean;
        AnnotationInstance annotation = method.annotation(RocketmqDotNames.ROCKETMQ_INCOMING);
        if (!Objects.isNull(annotation)) {
            AnnotationValue isOrderly = annotation.value("isOrderly");
            if (Objects.isNull(isOrderly)) {
                isOrderly = AnnotationValue.createBooleanValue("isOrderly", Boolean.FALSE);
            }

            this.isOrderly = isOrderly.asBoolean();
        }

        allow = Boolean.TRUE;
        return this;
    }

    public BeanMethodValidator checkParamter() {

        AnnotationInstance annotation = method.annotation(RocketmqDotNames.ROCKETMQ_INCOMING);
        if (Objects.isNull(annotation)) {
            allow = Boolean.FALSE;
            return this;
        }

        ClassInfo classInfo = method.declaringClass();
        AnnotationValue configKey = annotation.value("configKey");
        if (!Objects.isNull(configKey)) {
            if (Objects.isNull(pushConsumerConfig.additionalConfigs.get(configKey.asString()))) {
                throw new DeploymentConfigException("The configuration specified by " + configKey.asString() + " does not exist, declared class is "
                                + classInfo.name().toString());
            }
        }

        List<Type> parameters = method.parameters();
        for (Type type : parameters) {
            if (isOrderly && type.name().equals(RocketmqDotNames.CONCURRENTLY_CONTEXT)) {
                throw new DeploymentConfigException("Order mode listeners can only have the context parameter " + RocketmqDotNames.ORDERLY_CONTEXT);
            }

            if (!isOrderly && type.name().equals(RocketmqDotNames.ORDERLY_CONTEXT)) {
                throw new DeploymentConfigException("Concurrent mode listeners can only have the context parameter " + RocketmqDotNames.CONCURRENTLY_CONTEXT);
            }
        }

        return this;
    }

    public BeanMethodValidator checkReturnType() {
        Type type = method.returnType();
        if (type.name().equals(RETURN_FUTURE) || type.name().equals(RETURN_UNI)) {
            List<Type> arguments = type.asParameterizedType().arguments();
            checkRawType(arguments.get(0));
        } else {
            checkRawType(type);
        }

        return this;
    }

    private void checkRawType(Type type) {
        if (isOrderly && type.name().equals(CONCURRENTLY_STATUS)) {
            throw new DeploymentConfigException("Order mode listeners can only have the return type " + ORDERLY_STATUS);
        }

        if (!isOrderly && type.name().equals(ORDERLY_STATUS)) {
            throw new DeploymentConfigException("Concurrent mode listeners can only have the return type " + CONCURRENTLY_STATUS);
        }
    }

    public void to(BuildProducer<MetaClzBuilItem> metaMethods) {
        if (allow) {
            metaMethods.produce(new MetaClzBuilItem(bean, method));
        }
    }
}
