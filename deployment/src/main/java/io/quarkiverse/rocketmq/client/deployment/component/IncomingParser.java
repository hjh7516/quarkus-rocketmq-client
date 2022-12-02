package io.quarkiverse.rocketmq.client.deployment.component;

import io.quarkiverse.rocketmq.client.deployment.RocketmqDotNames;
import io.quarkiverse.rocketmq.client.runtime.ReturnType;
import io.quarkus.arc.processor.BeanInfo;
import io.quarkus.arc.processor.DotNames;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import io.quarkus.gizmo.FieldDescriptor;
import io.quarkus.gizmo.MethodCreator;
import io.quarkus.gizmo.MethodDescriptor;
import io.quarkus.gizmo.ResultHandle;
import io.quarkus.runtime.util.HashUtil;
import org.jboss.jandex.AnnotationValue;
import org.jboss.jandex.DotName;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.Type;

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Objects;

import static io.quarkiverse.rocketmq.client.deployment.RocketmqDotNames.CONCURRENTLY_CONTEXT;
import static io.quarkiverse.rocketmq.client.deployment.RocketmqDotNames.LIST;
import static io.quarkiverse.rocketmq.client.deployment.RocketmqDotNames.MESSAGE;
import static io.quarkiverse.rocketmq.client.deployment.RocketmqDotNames.ORDERLY_CONTEXT;
import static io.quarkiverse.rocketmq.client.deployment.RocketmqDotNames.RETURN_FUTURE;
import static io.quarkiverse.rocketmq.client.deployment.RocketmqDotNames.RETURN_UNI;
import static org.objectweb.asm.Opcodes.ACC_PROTECTED;

public class IncomingParser {

    static final String PROCESS_SUFFIX = "_MessageProcess";

    private BeanInfo bean;
    private MethodInfo method;

    public void next(BeanInfo bean, MethodInfo method) {
        this.bean = bean;
        this.method = method;
    }

    public String generateInvoker(ClassOutput classOutput) {
        String baseName;
        if (bean.getImplClazz().enclosingClass() != null) {
            baseName = DotNames.simpleName(bean.getImplClazz().enclosingClass()) + "_"
                    + DotNames.simpleName(bean.getImplClazz().name());
        } else {
            baseName = DotNames.simpleName(bean.getImplClazz().name());
        }
        StringBuilder sigBuilder = new StringBuilder();
        sigBuilder.append(method.name()).append("_").append(method.returnType().name().toString());
        for (Type i : method.parameters()) {
            sigBuilder.append(i.name().toString());
        }
        String targetPackage = DotNames.internalPackageNameWithTrailingSlash(bean.getImplClazz().name());
        String generatedName = targetPackage + baseName
                + PROCESS_SUFFIX + "_" + method.name() + "_"
                + HashUtil.sha1(sigBuilder.toString());

        generateStandardInvoker(method, classOutput, generatedName);
        return generatedName.replace('/', '.');
    }

    private void generateStandardInvoker(MethodInfo method, ClassOutput classOutput, String generatedName) {
        AnnotationValue isOrderly = method.annotation(RocketmqDotNames.INCOMING).value("isOrderly");
        if (Objects.isNull(isOrderly)) {
            isOrderly = AnnotationValue.createBooleanValue("isOrderly", false);
        }
        DotName superClz = isOrderly.asBoolean() ? RocketmqDotNames.PROCESS_ORDERLY : RocketmqDotNames.PROCESS_CONCURRENTLY;
        try (ClassCreator invoker = ClassCreator.builder().classOutput(classOutput).className(generatedName)
                .superClass(superClz.toString())
                .build()) {

            String beanInstanceType = method.declaringClass().name().toString();
            FieldDescriptor beanInstanceField = invoker.getFieldCreator("beanInstance", beanInstanceType)
                    .getFieldDescriptor();

            try (MethodCreator ctor = invoker.getMethodCreator("<init>", void.class, Object.class)) {
                ctor.setModifiers(Modifier.PUBLIC);
                ctor.invokeSpecialMethod(MethodDescriptor.ofConstructor(superClz.toString()), ctor.getThis());
                ResultHandle self = ctor.getThis();
                ResultHandle beanInstance = ctor.getMethodParam(0);
                ctor.writeInstanceField(beanInstanceField, self, beanInstance);
                ctor.returnValue(null);
            }

            try (MethodCreator invoke = invoker.getMethodCreator(
                    MethodDescriptor.ofMethod(generatedName, "process", Object.class, Object[].class))) {

                invoke.setModifiers(ACC_PROTECTED);
                int parametersCount = method.parameters().size();
                String[] argTypes = new String[parametersCount];
                ResultHandle[] args = new ResultHandle[parametersCount];
                List<Type> parameters = method.parameters();
                for (int parameterIdx = 0; parameterIdx < parameters.size(); parameterIdx ++) {
                    Type parameter = parameters.get(parameterIdx);
                    if (parameter.name().equals(LIST) &&
                            MESSAGE.equals(parameter.asParameterizedType().arguments().get(0).name())) {
                        args[parameterIdx] = invoke.readArrayValue(invoke.getMethodParam(0), 0);
                    }else if (parameter.name().equals(CONCURRENTLY_CONTEXT)
                            || parameter.name().equals(ORDERLY_CONTEXT)) {
                        args[parameterIdx] = invoke.readArrayValue(invoke.getMethodParam(0), 1);
                    }else {
                        args[parameterIdx] = loadZeroValue(parameter, invoke);
                    }

                    argTypes[parameterIdx] = method.parameters().get(parameterIdx).name().toString();
                }
                ResultHandle result = invoke.invokeVirtualMethod(
                        MethodDescriptor.ofMethod(beanInstanceType, method.name(),
                                method.returnType().name().toString(), argTypes),
                        invoke.readInstanceField(beanInstanceField, invoke.getThis()), args);
                invoke.returnValue(result);
            }

            try (MethodCreator returnType = invoker
                    .getMethodCreator(MethodDescriptor.ofMethod(generatedName, "returnType", ReturnType.class))) {
                returnType.setModifiers(ACC_PROTECTED);
                Type type = method.returnType();
                if (type.name().equals(RETURN_FUTURE)) {
                    returnType.returnValue(returnType.load(ReturnType.FUTURE));
                } else if (type.name().equals(RETURN_UNI)) {
                    returnType.returnValue(returnType.load(ReturnType.UNI));
                } else {
                    returnType.returnValue(returnType.load(ReturnType.OTHER));
                }
            }
        }
    }

    private ResultHandle loadZeroValue(Type parameter, MethodCreator methodCreator) {

        ResultHandle handle;
        DotName name = parameter.name();
        switch (name.toString()) {
            case "int":
            case "char":
            case "byte":
            case "short":
                handle = methodCreator.load(0);
                break;
            case "float":
                handle = methodCreator.load(0F);
                break;
            case "double":
                handle = methodCreator.load(0D);
                break;
            case "long":
                handle = methodCreator.load(0L);
                break;
            case "boolean":
                handle = methodCreator.load(false);
                break;
            default:
                handle = methodCreator.loadNull();
        }

        return handle;
    }
}
