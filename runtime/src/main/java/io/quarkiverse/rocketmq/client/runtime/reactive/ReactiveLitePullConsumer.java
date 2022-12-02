package io.quarkiverse.rocketmq.client.runtime.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import io.smallrye.mutiny.Uni;
import io.vertx.rocketmq.client.consumer.MessageSelector;

public interface ReactiveLitePullConsumer {

    /**
     * Subscribe some topic with subExpression
     *
     * @param subExpression subscription expression.it only support or operation such as "tag1 || tag2 || tag3" <br>
     *        if
     *        null or * expression,meaning subscribe all
     */
    Uni<Void> subscribe(final String topic, final String subExpression);

    /**
     * Subscribe some topic with selector.
     *
     * @param selector message selector({@link MessageSelector}), can be null.
     */
    Uni<Void> subscribe(final String topic, final MessageSelector selector);

    /**
     * Unsubscribe consumption some topic
     *
     * @param topic Message topic that needs to be unsubscribe.
     */
    Uni<Void> unsubscribe(final String topic);

    /**
     * Manually assign a list of message queues to this consumer. This interface does not allow for incremental
     * assignment and will replace the previous assignment (if there is one).
     *
     * @param messageQueues Message queues that needs to be assigned.
     */
    Uni<Void> assign(Collection<MessageQueue> messageQueues);

    /**
     * Fetch data for the topics or partitions specified using assign API
     *
     * @return list of message, can be null.
     */
    Uni<List<MessageExt>> poll();

    /**
     * Fetch data for the topics or partitions specified using assign API
     *
     * @param timeout The amount time, in milliseconds, spent waiting in poll if data is not available. Must not be
     *        negative
     * @return list of message, can be null.
     */
    Uni<List<MessageExt>> poll(long timeout);

    /**
     * Overrides the fetch offsets that the consumer will use on the next poll. If this API is invoked for the same
     * message queue more than once, the latest offset will be used on the next poll(). Note that you may lose data if
     * this API is arbitrarily used in the middle of consumption.
     *
     * @param messageQueue messageQueue
     * @param offset offset
     */
    Uni<Void> seek(MessageQueue messageQueue, long offset);

    /**
     * Get metadata about the message queues for a given topic.
     *
     * @param topic The topic that need to get metadata.
     * @return collection of message queues
     */
    Uni<Set<MessageQueue>> fetchMessageQueues(String topic);

    /**
     * Manually commit consume offset.
     */
    Uni<Void> commitSync();

    /**
     * Get the last committed offset for the given message queue.
     *
     * @param messageQueue messageQueue
     * @return offset, if offset equals -1 means no offset in broker.
     */
    Uni<Long> committed(MessageQueue messageQueue);

    /**
     * Overrides the fetch offsets with the begin offset that the consumer will use on the next poll. If this API is
     * invoked for the same message queue more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption.
     *
     */
    Uni<Void> seekToBegin(MessageQueue messageQueue);

    /**
     * Overrides the fetch offsets with the end offset that the consumer will use on the next poll. If this API is
     * invoked for the same message queue more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption.
     *
     */
    Uni<Void> seekToEnd(MessageQueue messageQueue);
}
