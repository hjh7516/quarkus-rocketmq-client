package io.quarkiverse.rocketmq.client.runtime;

public class MessageProcessConcurrentlyTest extends MessageProcessConcurrently {

    private Object obj;

    public MessageProcessConcurrentlyTest(Object obj) {
        this.obj = obj;
    }

    @Override
    protected ReturnType returnType() {
        return null;
    }

    @Override
    protected Object process(Object... args) {
        return null;
    }
}
