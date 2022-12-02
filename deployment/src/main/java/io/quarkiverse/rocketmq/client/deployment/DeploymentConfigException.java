package io.quarkiverse.rocketmq.client.deployment;

public class DeploymentConfigException extends RuntimeException {

    private final String message;

    public DeploymentConfigException(String message) {
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
