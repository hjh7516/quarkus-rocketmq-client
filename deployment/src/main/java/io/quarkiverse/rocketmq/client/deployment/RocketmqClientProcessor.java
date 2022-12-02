package io.quarkiverse.rocketmq.client.deployment;

import io.quarkiverse.rocketmq.client.deployment.component.BeanMethodValidator;
import io.quarkiverse.rocketmq.client.deployment.component.IncomingParser;
import io.quarkiverse.rocketmq.client.deployment.items.ConsumerBuilItem;
import io.quarkiverse.rocketmq.client.deployment.items.MetaClzBuilItem;
import io.quarkiverse.rocketmq.client.runtime.RocketmqClientUtil;
import io.quarkiverse.rocketmq.client.runtime.RocketmqListener;
import io.quarkiverse.rocketmq.client.runtime.RocketmqProducer;
import io.quarkiverse.rocketmq.client.runtime.RocketmqRecorder;
import io.quarkiverse.rocketmq.client.runtime.config.ProducerConfig;
import io.quarkiverse.rocketmq.client.runtime.config.ProducerConfig.ProducerConfiguration;
import io.quarkiverse.rocketmq.client.runtime.config.PullConsumerConfig;
import io.quarkiverse.rocketmq.client.runtime.config.PullConsumerConfig.PullConfiguration;
import io.quarkiverse.rocketmq.client.runtime.config.PushConsumerConfig;
import io.quarkiverse.rocketmq.client.runtime.reactive.LitePullConsumer;
import io.quarkiverse.rocketmq.client.runtime.reactive.MQProducer;
import io.quarkiverse.rocketmq.client.runtime.reactive.MQPullConsumer;
import io.quarkiverse.rocketmq.client.runtime.reactive.ReactiveLitePullConsumer;
import io.quarkiverse.rocketmq.client.runtime.reactive.ReactiveMQProducer;
import io.quarkiverse.rocketmq.client.runtime.reactive.ReactiveMQPullConsumer;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.BeanArchiveIndexBuildItem;
import io.quarkus.arc.deployment.BeanContainerBuildItem;
import io.quarkus.arc.deployment.BeanDiscoveryFinishedBuildItem;
import io.quarkus.arc.deployment.BeanRegistrationPhaseBuildItem;
import io.quarkus.arc.deployment.SyntheticBeanBuildItem;
import io.quarkus.arc.deployment.UnremovableBeanBuildItem;
import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.arc.processor.BeanRegistrar;
import io.quarkus.deployment.GeneratedClassGizmoAdaptor;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.Consume;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.GeneratedClassBuildItem;
import io.quarkus.deployment.builditem.QuarkusBuildCloseablesBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.gizmo.ClassOutput;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.vertx.core.deployment.CoreVertxBuildItem;
import io.vertx.rocketmq.client.consumer.MQPushConsumer;
import io.vertx.rocketmq.client.consumer.listener.MessageListener;
import io.vertx.rocketmq.client.exception.MQClientException;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.MethodInfo;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Default;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

class RocketmqClientProcessor {

    private static final String FEATURE = "quarkus-rocketmq-client";

    private static final DotName CLIENT_ANNOTATION = DotName.createSimple(RocketmqProducer.class.getName());
    private static final DotName LISTENER_INTERFACE = DotName.createSimple(MessageListener.class.getName());
    private static final DotName PUSH_CONSUMER_ANNOTATION = DotName.createSimple(RocketmqListener.class.getName());

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    List<AdditionalBeanBuildItem> registerRedisBeans() {
        return Arrays.asList(
                AdditionalBeanBuildItem
                        .builder()
                        .addBeanClass(RocketmqProducer.class)
                        .build(),
                AdditionalBeanBuildItem
                        .builder()
                        .addBeanClass(RocketmqListener.class)
                        .build());
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    void produceClient(RocketmqRecorder recorder,
            CoreVertxBuildItem vertx,
            QuarkusBuildCloseablesBuildItem closeablesBuildItem,
            BuildProducer<SyntheticBeanBuildItem> syntheticBeans,
            ProducerConfig config) throws MQClientException {
        Map<String, ProducerConfiguration> configurations = new HashMap<>();
        if (!Objects.isNull(config.defaultConfig)) {
            configurations.put(RocketmqClientUtil.DEFAULT_CLIENT, config.defaultConfig);
        }
        configurations.putAll(config.additionalConfigs);

        for (String name : configurations.keySet()) {
            ProducerConfiguration configuration = configurations.get(name);
            RuntimeValue<io.vertx.rocketmq.client.producer.MQProducer> producer = recorder.startProducer(configuration,
                    vertx.getVertx());

            RuntimeValue<ReactiveMQProducer> reactiveProducer = recorder.createReactiveProducer(producer);
            syntheticBeans.produce(buildSyntheticBean(name, ReactiveMQProducer.class, () -> reactiveProducer));

            syntheticBeans.produce(buildSyntheticBean(name, MQProducer.class, () -> recorder.createProducer(reactiveProducer)));

            closeablesBuildItem.add(() -> producer.getValue().shutdown());
        }
    }

    //    @BuildStep
    //    @Record(ExecutionTime.RUNTIME_INIT)
    void startPullConsumer(RocketmqRecorder recorder,
            CoreVertxBuildItem vertx,
            QuarkusBuildCloseablesBuildItem closeablesBuildItem,
            BuildProducer<SyntheticBeanBuildItem> syntheticBeans,
            PullConsumerConfig config) throws MQClientException {

        Map<String, PullConfiguration> configurations = new HashMap<>();
        if (!Objects.isNull(config.defaultConfig)) {
            configurations.put(RocketmqClientUtil.DEFAULT_CLIENT, config.defaultConfig);
        }
        configurations.putAll(config.additionalConfigs);

        for (String name : configurations.keySet()) {
            PullConfiguration configuration = configurations.get(name);
            RuntimeValue<io.vertx.rocketmq.client.consumer.MQPullConsumer> pullConsumer = recorder
                    .startPullConsumer(configuration, vertx.getVertx());
            RuntimeValue<ReactiveMQPullConsumer> reactivePullConsumer = recorder.createReactivePullConsumer(pullConsumer);
            syntheticBeans.produce(buildSyntheticBean(name, ReactiveMQPullConsumer.class, () -> reactivePullConsumer));
            syntheticBeans.produce(
                    buildSyntheticBean(name, MQPullConsumer.class, () -> recorder.createPullConsumer(reactivePullConsumer)));
            closeablesBuildItem.add(() -> pullConsumer.getValue().shutdown());

            RuntimeValue<io.vertx.rocketmq.client.consumer.LitePullConsumer> litePullConsumer = recorder
                    .startLitePullConsumer(configuration, vertx.getVertx());
            RuntimeValue<ReactiveLitePullConsumer> reactiveLitePullConsumer = recorder
                    .createReactiveLitePullConsumer(litePullConsumer);
            syntheticBeans.produce(buildSyntheticBean(name, ReactiveLitePullConsumer.class, () -> reactiveLitePullConsumer));
            syntheticBeans.produce(
                    buildSyntheticBean(name, LitePullConsumer.class,
                            () -> recorder.createLitePullConsumer(reactiveLitePullConsumer)));
            closeablesBuildItem.add(() -> litePullConsumer.getValue().shutdown());
        }
    }

    private SyntheticBeanBuildItem buildSyntheticBean(String name, Class<?> scoped, Supplier<RuntimeValue<?>> supplier) {
        SyntheticBeanBuildItem.ExtendedBeanConfigurator configurator = SyntheticBeanBuildItem
                .configure(scoped)
                .scope(ApplicationScoped.class)
                .runtimeValue(supplier.get())
                .setRuntimeInit();

        if (RocketmqClientUtil.isDefault(name)) {
            configurator.addQualifier(Default.class);
        } else {
            configurator.addQualifier().annotation(CLIENT_ANNOTATION).addValue("name", name).done();
        }

        return configurator.done();
    }

    @BuildStep
    void markerUnRemovalBeanListner(BuildProducer<UnremovableBeanBuildItem> unremovableBean,
            BeanArchiveIndexBuildItem indexBuildItem, PushConsumerConfig pushConsumerConfig) {
        IndexView indexView = indexBuildItem.getIndex();
        Collection<ClassInfo> implementors = indexView.getKnownClasses();
        Set<DotName> needListners = new HashSet<>();
        for (ClassInfo clzInfo : implementors) {
            AnnotationInstance annotation = clzInfo.classAnnotation(PUSH_CONSUMER_ANNOTATION);
            if (!Objects.isNull(annotation)) {
                AnnotationValue configKey = annotation.value("configKey");
                if (!Objects.isNull(configKey) && !RocketmqClientUtil.isDefault(configKey.asString())) {
                    PushConsumerConfig.PushConfiguration configuration = pushConsumerConfig.additionalConfigs
                            .get(configKey.asString());
                    if (Objects.isNull(configuration)) {
                        throw new DeploymentConfigException("The configuration specified by " + configKey.asString() + " does not exist");
                    }
                }
                needListners.add(clzInfo.name());
            } else {
                List<MethodInfo> methods = clzInfo.methods();
                for (MethodInfo method : methods) {
                    AnnotationInstance incoming = method.annotation(RocketmqDotNames.ROCKETMQ_INCOMING);
                    if (Objects.isNull(incoming)) {
                        needListners.add(clzInfo.name());
                        break;
                    }
                }
            }
        }

        needListners.forEach(listener -> unremovableBean.produce(UnremovableBeanBuildItem.beanTypes(listener)));
    }

    @BuildStep
    void collectMessageProcess(BeanDiscoveryFinishedBuildItem beanDiscoveryFinished,
            BuildProducer<MetaClzBuilItem> metaMethods,
            PushConsumerConfig pushConsumerConfig) {

        BeanMethodValidator validator = new BeanMethodValidator(pushConsumerConfig);
        for (BeanInfo bean : beanDiscoveryFinished.beanStream().classBeans()) {
            for (MethodInfo method : bean.getTarget().get().asClass().methods()) {
                validator.from(bean, method)
                        .checkParamter()
                        .checkReturnType()
                        .to(metaMethods);
            }
        }
    }

    @BuildStep
    void createMessageProcess(List<MetaClzBuilItem> metaBuilItems,
            BuildProducer<GeneratedClassBuildItem> generatedClass,
            BuildProducer<ConsumerBuilItem> consumerProducer,
            BuildProducer<ReflectiveClassBuildItem> reflectiveClass) {

        ClassOutput classOutput = new GeneratedClassGizmoAdaptor(generatedClass, true);
        IncomingParser parser = new IncomingParser();

        for (MetaClzBuilItem metaClzBuilItem : metaBuilItems) {
            parser.next(metaClzBuilItem.getBean(), metaClzBuilItem.getMethod());
            String generatedInvokerName = parser.generateInvoker(classOutput);
            AnnotationInstance annotation = metaClzBuilItem.getMethod().annotation(RocketmqDotNames.INCOMING);
            AnnotationValue configKey = annotation.value("configKey");
            if (Objects.isNull(configKey)) {
                configKey = AnnotationValue.createStringValue("configKey", RocketmqClientUtil.DEFAULT_CLIENT);
            }
            reflectiveClass.produce(new ReflectiveClassBuildItem(false, false, generatedInvokerName));
            consumerProducer.produce(new ConsumerBuilItem(metaClzBuilItem.getBean().getIdentifier(), configKey.asString(),
                    generatedInvokerName));
        }
    }

    @BuildStep
    @Consume(BeanContainerBuildItem.class)
    @Record(ExecutionTime.RUNTIME_INIT)
    void startPushConsumer(BeanRegistrationPhaseBuildItem beanRegistrationPhaseBuildItem,
            CoreVertxBuildItem vertx,
            QuarkusBuildCloseablesBuildItem closeablesBuildItem,
            RocketmqRecorder recorder,
            PushConsumerConfig pushConsumerConfig,
            List<ConsumerBuilItem> consumerBuilItems) throws MQClientException {

        for (ConsumerBuilItem consumerBuilItem : consumerBuilItems) {
            PushConsumerConfig.PushConfiguration configuration;
            if (RocketmqClientUtil.isDefault(consumerBuilItem.getConfigKey())) {
                configuration = pushConsumerConfig.defaultConfig;
            } else {
                configuration = pushConsumerConfig.additionalConfigs.get(consumerBuilItem.getConfigKey());
            }
            if (configuration.topic.isEmpty()) {
                continue;
            }
            RuntimeValue<MQPushConsumer> consumer = recorder.startPushConsumer(configuration, vertx.getVertx(),
                    consumerBuilItem.getIdentifier(), consumerBuilItem.getImplClz());
            closeablesBuildItem.add(() -> consumer.getValue().shutdown());
        }

        BeanRegistrar.RegistrationContext context = beanRegistrationPhaseBuildItem.getContext();
        List<BeanInfo> beanInfos = context.beans().withQualifier(PUSH_CONSUMER_ANNOTATION).collect();
        if (null == beanInfos || beanInfos.isEmpty()) {
            return;
        }

        for (BeanInfo beanInfo : beanInfos) {
            Optional<AnnotationInstance> qualifier = beanInfo.getQualifier(PUSH_CONSUMER_ANNOTATION);
            if (qualifier.isPresent()) {
                AnnotationValue configKey = qualifier.get().value("configKey");
                PushConsumerConfig.PushConfiguration configuration = pushConsumerConfig.defaultConfig;
                if (!Objects.isNull(configKey) && !RocketmqClientUtil.isDefault(configKey.asString())) {
                    configuration = pushConsumerConfig.additionalConfigs.get(configKey.asString());
                    if (Objects.isNull(configuration)) {
                        throw new DeploymentConfigException("The configuration specified by " + configKey.asString() + " does not exist");
                    }
                }

                if (configuration.topic.isEmpty()) {
                    continue;
                }
                RuntimeValue<MQPushConsumer> consumer = recorder.startPushConsumer(configuration, vertx.getVertx(),
                        beanInfo.getIdentifier(), null);
                closeablesBuildItem.add(() -> consumer.getValue().shutdown());
            }
        }
    }


}
