package io.quarkiverse.rocketmq.client.runtime.reactive;

import java.util.Set;

import org.apache.rocketmq.common.message.MessageQueue;

import io.vertx.rocketmq.client.consumer.MessageSelector;
import io.vertx.rocketmq.client.consumer.PullResult;

public interface MQPullConsumer {

    /**
     * Pulling the messages,not blocking
     *
     * @param mq from which message queue
     * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br>
     *        if
     *        null or * expression,meaning subscribe
     *        all
     * @param offset from where to pull
     * @param maxNums max pulling numbers
     * @return The resulting {@code PullRequest}
     */
    PullResult pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums);

    /**
     * Pulling the messages in the specified timeout
     *
     * @return The resulting {@code PullRequest}
     */
    PullResult pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums,
            final long timeout);

    /**
     * Pulling the messages, not blocking
     * <p>
     * support other message selection, such as {@link org.apache.rocketmq.common.filter.ExpressionType#SQL92}
     * </p>
     *
     * @param mq from which message queue
     * @param selector message selector({@link MessageSelector}), can be null.
     * @param offset from where to pull
     * @param maxNums max pulling numbers
     * @return The resulting {@code PullRequest}
     */
    PullResult pull(final MessageQueue mq, final MessageSelector selector, final long offset, final int maxNums);

    /**
     * Pulling the messages in the specified timeout
     * <p>
     * support other message selection, such as {@link org.apache.rocketmq.common.filter.ExpressionType#SQL92}
     * </p>
     *
     * @param mq from which message queue
     * @param selector message selector({@link MessageSelector}), can be null.
     * @param offset from where to pull
     * @param maxNums max pulling numbers
     * @param timeout Pulling the messages in the specified timeout
     * @return The resulting {@code PullRequest}
     */
    PullResult pull(final MessageQueue mq, final MessageSelector selector, final long offset, final int maxNums,
            final long timeout);

    /**
     * Pulling the messages,if no message arrival,blocking some time
     *
     * @return The resulting {@code PullRequest}
     */
    PullResult pullBlockIfNotFound(final MessageQueue mq, final String subExpression, final long offset, final int maxNums);

    /**
     * Fetch message queues from consumer cache according to the topic
     *
     * @param topic message topic
     * @return queue set
     */
    Set<MessageQueue> fetchSubscribeMessageQueues(final String topic);

}
