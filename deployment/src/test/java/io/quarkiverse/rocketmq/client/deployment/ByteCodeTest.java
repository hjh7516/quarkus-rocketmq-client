package io.quarkiverse.rocketmq.client.deployment;

import io.quarkiverse.rocketmq.client.runtime.ReturnType;
import io.quarkus.gizmo.ClassCreator;
import io.quarkus.gizmo.ClassOutput;
import io.quarkus.gizmo.FieldDescriptor;
import io.quarkus.gizmo.MethodCreator;
import io.quarkus.gizmo.MethodDescriptor;
import io.quarkus.gizmo.ResultHandle;
import org.jboss.jandex.DotName;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Modifier;

import static org.objectweb.asm.Opcodes.ACC_PROTECTED;

public class ByteCodeTest {

    public static void main(String[] args) {
        ClassOutput classOutput = new GeneratedClass();
        generateStandardInvoker(classOutput, "com.ms.mq.RocketmqProcess_MessageProcess_process_test");
    }

    private static void generateStandardInvoker(ClassOutput classOutput, String generatedName) {
        try (ClassCreator invoker = ClassCreator.builder().classOutput(classOutput).className(generatedName)
                .superClass(RocketmqDotNames.PROCESS_CONCURRENTLY.toString())
                .build()) {

            DotName beanInstanceType = DotName.createSimple("io.quarkiverse.rocketmq.client.deployment.MessageProcess");
            FieldDescriptor beanInstanceField = invoker.getFieldCreator("beanInstance", beanInstanceType.toString())
                    .getFieldDescriptor();

            try (MethodCreator ctor = invoker.getMethodCreator("<init>", void.class, Object.class)) {
                ctor.setModifiers(Modifier.PUBLIC);
                ctor.invokeSpecialMethod(MethodDescriptor.ofConstructor(RocketmqDotNames.PROCESS_CONCURRENTLY.toString()),
                        ctor.getThis());
                ResultHandle self = ctor.getThis();
                ResultHandle beanInstance = ctor.getMethodParam(0);
                ctor.writeInstanceField(beanInstanceField, self, beanInstance);
                ctor.returnValue(null);
            }

            try (MethodCreator invoke = invoker.getMethodCreator(
                    MethodDescriptor.ofMethod(generatedName, "process", Object.class, Object[].class))) {

                ResultHandle[] args = new ResultHandle[1];
                args[0] = invoke.readArrayValue(invoke.getMethodParam(0), 0);

                invoke.setModifiers(ACC_PROTECTED);
                ResultHandle result = invoke.invokeVirtualMethod(
                        MethodDescriptor.ofMethod(beanInstanceType.toString(), "process",
                                "java.lang.Object", "java.util.List"),
                        invoke.readInstanceField(beanInstanceField, invoke.getThis()), args);
                invoke.returnValue(result);
            }

            try (MethodCreator returnType = invoker
                    .getMethodCreator(MethodDescriptor.ofMethod(generatedName, "returnType", ReturnType.class))) {
                returnType.setModifiers(ACC_PROTECTED);
                returnType.returnValue(returnType.load(ReturnType.FUTURE));
            }
        }
    }

    static class GeneratedClass implements ClassOutput {

        @Override
        public void write(String name, byte[] data) {
            File debugPath = new File("D:\\workspace\\contributor\\quarkus-rocketmq-client\\target");
            File classFile = new File(debugPath, name + ".class");
            classFile.getParentFile().mkdirs();
            try (FileOutputStream classWriter = new FileOutputStream(classFile)) {
                classWriter.write(data);
            } catch (Exception e) {
                e.printStackTrace();
            }

//            MyClasslorder classlorder = new MyClasslorder(name.replace('/', '.'), data);
//            try {
//                Class<?> aClass = classlorder.loadClass(name.replace('/', '.'));
//                Constructor<?> constructor = aClass.getConstructor(Object.class);
//                MessageProcess messageProcess = new MessageProcess();
//                Object o = constructor.newInstance(messageProcess);
//                Method consumeMessage = aClass.getMethod("consumeMessage", List.class, ConsumeConcurrentlyContext.class);
//
//                List<MessageExt> messageExts = new ArrayList<>();
//                messageExts.add(new MessageExt());
//
//                MessageQueue messageQueue = new MessageQueue();
//                ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
//                consumeMessage.invoke(o, messageExts, context);
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
        }
    }

    static class MyClasslorder extends ClassLoader {
        private String name;
        private byte[] data;

        public MyClasslorder(String name, byte[] data) {
            this.name = name;
            this.data = data;
        }

        @Override
        protected Class<?> findClass(String name) {
            return defineClass(null, data, 0, data.length);
        }
    }
}
