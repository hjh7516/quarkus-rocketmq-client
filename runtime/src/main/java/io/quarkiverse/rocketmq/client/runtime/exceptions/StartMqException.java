package io.quarkiverse.rocketmq.client.runtime.exceptions;

public class StartMqException extends RuntimeException {

    private final String message;

    public StartMqException(String message, Throwable throwable) {
        super(throwable);
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
