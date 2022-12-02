package io.quarkiverse.rocketmq.client.runtime;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RocketmqIncoming {

    boolean isOrderly() default false;

    String configKey() default RocketmqClientUtil.DEFAULT_CLIENT;
}
