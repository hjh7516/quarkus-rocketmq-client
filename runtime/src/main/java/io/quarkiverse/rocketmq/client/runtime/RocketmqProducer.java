package io.quarkiverse.rocketmq.client.runtime;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.inject.Qualifier;

@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER })
@Retention(RUNTIME)
@Documented
@Qualifier
public @interface RocketmqProducer {

    String name() default "";
}
