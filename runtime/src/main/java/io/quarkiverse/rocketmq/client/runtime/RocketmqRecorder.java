package io.quarkiverse.rocketmq.client.runtime;

import io.quarkiverse.rocketmq.client.runtime.config.ProducerConfig.ProducerConfiguration;
import io.quarkiverse.rocketmq.client.runtime.config.PullConsumerConfig.PullConfiguration;
import io.quarkiverse.rocketmq.client.runtime.config.PushConsumerConfig;
import io.quarkiverse.rocketmq.client.runtime.exceptions.StartMqException;
import io.quarkiverse.rocketmq.client.runtime.reactive.ReactiveLitePullConsumer;
import io.quarkiverse.rocketmq.client.runtime.reactive.ReactiveMQProducer;
import io.quarkiverse.rocketmq.client.runtime.reactive.ReactiveMQPullConsumer;
import io.quarkiverse.rocketmq.client.runtime.reactive.impl.ReactiveMQProducerImpl;
import io.quarkiverse.rocketmq.client.runtime.reactive.impl.SimpleLitePullConsumer;
import io.quarkiverse.rocketmq.client.runtime.reactive.impl.SimpleMQProducer;
import io.quarkiverse.rocketmq.client.runtime.reactive.impl.SimpleMQPullConsumer;
import io.quarkiverse.rocketmq.client.runtime.reactive.impl.SimpleReactiveLitePullConsumer;
import io.quarkiverse.rocketmq.client.runtime.reactive.impl.SimpleReactiveMQPullConsumer;
import io.quarkus.arc.Arc;
import io.quarkus.arc.InjectableBean;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import io.vertx.rocketmq.client.common.RocketmqOptions;
import io.vertx.rocketmq.client.consumer.DefaultLitePullConsumer;
import io.vertx.rocketmq.client.consumer.DefaultMQPullConsumer;
import io.vertx.rocketmq.client.consumer.DefaultMQPushConsumer;
import io.vertx.rocketmq.client.consumer.LitePullConsumer;
import io.vertx.rocketmq.client.consumer.MQPullConsumer;
import io.vertx.rocketmq.client.consumer.MQPushConsumer;
import io.vertx.rocketmq.client.consumer.listener.MessageListener;
import io.vertx.rocketmq.client.exception.MQClientException;
import io.vertx.rocketmq.client.producer.MQProducer;
import io.vertx.rocketmq.client.producer.TransactionMQProducer;
import org.jboss.logging.Logger;

import java.lang.reflect.Constructor;
import java.util.Objects;
import java.util.function.Supplier;

@Recorder
public class RocketmqRecorder {

    private static final Logger LOGGER = Logger.getLogger(RocketmqRecorder.class);

    public RuntimeValue<MQProducer> startProducer(ProducerConfiguration configuration, Supplier<Vertx> vertxSupplier)
            throws MQClientException {
        RocketmqOptions options = RocketmqClientUtil.buildOptions(configuration);
        MQProducer producer = new TransactionMQProducer(options.getGroupName(), (VertxInternal) vertxSupplier.get(), options);
        producer.start();
        return new RuntimeValue<>(producer);
    }

    public RuntimeValue<ReactiveMQProducer> createReactiveProducer(RuntimeValue<MQProducer> producer) {
        ReactiveMQProducer reactiveMQProducer = new ReactiveMQProducerImpl(producer.getValue());
        return new RuntimeValue<>(reactiveMQProducer);
    }

    public RuntimeValue<SimpleMQProducer> createProducer(RuntimeValue<ReactiveMQProducer> reactiveMQProducer) {
        return new RuntimeValue<>(new SimpleMQProducer(reactiveMQProducer.getValue()));
    }

    public RuntimeValue<MQPullConsumer> startPullConsumer(PullConfiguration configuration, Supplier<Vertx> vertxSupplier)
            throws MQClientException {
        RocketmqOptions options = RocketmqClientUtil.buildOptions(configuration);
        MQPullConsumer pullConsumer = new DefaultMQPullConsumer(options.getGroupName(), (VertxInternal) vertxSupplier.get(),
                options);
        pullConsumer.start();
        return new RuntimeValue<>(pullConsumer);
    }

    public RuntimeValue<ReactiveMQPullConsumer> createReactivePullConsumer(RuntimeValue<MQPullConsumer> pullConsumer) {
        ReactiveMQPullConsumer reactiveMQPullConsumer = new SimpleReactiveMQPullConsumer(pullConsumer.getValue());
        return new RuntimeValue<>(reactiveMQPullConsumer);
    }

    public RuntimeValue<SimpleMQPullConsumer> createPullConsumer(RuntimeValue<ReactiveMQPullConsumer> pullConsumer) {
        return new RuntimeValue<>(new SimpleMQPullConsumer(pullConsumer.getValue()));
    }

    public RuntimeValue<LitePullConsumer> startLitePullConsumer(PullConfiguration configuration, Supplier<Vertx> vertxSupplier)
            throws MQClientException {
        RocketmqOptions options = RocketmqClientUtil.buildOptions(configuration);
        LitePullConsumer litePullConsumer = new DefaultLitePullConsumer(options.getGroupName(), null,
                (VertxInternal) vertxSupplier.get(), options);
        litePullConsumer.start();
        return new RuntimeValue<>(litePullConsumer);
    }

    public RuntimeValue<ReactiveLitePullConsumer> createReactiveLitePullConsumer(
            RuntimeValue<LitePullConsumer> litePullConsumer) {
        return new RuntimeValue<>(new SimpleReactiveLitePullConsumer(litePullConsumer.getValue()));
    }

    public RuntimeValue<SimpleLitePullConsumer> createLitePullConsumer(
            RuntimeValue<ReactiveLitePullConsumer> litePullConsumer) {
        return new RuntimeValue<>(new SimpleLitePullConsumer(litePullConsumer.getValue()));
    }

    public RuntimeValue<MQPushConsumer> startPushConsumer(PushConsumerConfig.PushConfiguration configuration,
            Supplier<Vertx> vertxSupplier, String beanIdentifier, String consumerName) throws MQClientException {
        RocketmqOptions options = RocketmqClientUtil.buildOptions(configuration);
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(options.getGroupName(), (VertxInternal) vertxSupplier.get(),
                options);
        consumer.setConsumeFromWhere(options.getConsumeFromWhere());
        consumer.subscribe(options.getSubscriptionTopic(), options.getSubscriptionTag());

        InjectableBean<Object> injectableBean = Arc.container().bean(beanIdentifier);
        Object messageListener = injectableBean.get(null);
        if (!Objects.isNull(consumerName)) {
            try {
                Class<?> clz = Thread.currentThread().getContextClassLoader().loadClass(consumerName);
                Constructor<?> constructor = clz.getConstructor(Object.class);
                consumer.setMessageListener((MessageListener) constructor.newInstance(messageListener));
            } catch (Exception e) {
                throw new StartMqException("Load class [" + consumerName + "] faile", e);
            }
        } else {
            consumer.setMessageListener((MessageListener) messageListener);
        }
        consumer.start();

        return new RuntimeValue<>(consumer);
    }
}
