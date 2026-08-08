package org.abhi.kafkasdk.core.service;

import org.abhi.kafkasdk.core.ITopicPublish;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Contract for publishing messages to Kafka topics defined as {@link ITopicPublish} interfaces.
 */
public interface IPublishService {

    /**
     * Publishes a message to the binding declared by the given topic type.
     *
     * @param topicType topic interface type
     * @param message   payload to publish
     */
    void publish(Class<? extends ITopicPublish> topicType, Object message);

    /**
     * Publishes a message with additional headers merged over the topic defaults.
     *
     * @param topicType topic interface type
     * @param message   payload to publish
     * @param headers   additional headers (overrides topic defaults)
     */
    void publish(Class<? extends ITopicPublish> topicType, Object message, Map<String, Object> headers);

    /**
     * Publishes a message asynchronously on a shared executor.
     *
     * @param topicType topic interface type
     * @param message   payload to publish
     * @return a future completing when the send is dispatched
     */
    CompletableFuture<Void> publishAsync(Class<? extends ITopicPublish> topicType, Object message);

    /**
     * Publishes a message asynchronously with additional headers on a shared executor.
     *
     * @param topicType topic interface type
     * @param message   payload to publish
     * @param headers   additional headers (overrides topic defaults)
     * @return a future completing when the send is dispatched
     */
    CompletableFuture<Void> publishAsync(Class<? extends ITopicPublish> topicType, Object message, Map<String, Object> headers);

    /**
     * Publishes a message asynchronously using the supplied executor.
     *
     * @param topicType topic interface type
     * @param message   payload to publish
     * @param executor  executor to run the publish on
     * @return a future completing when the send is dispatched
     */
    CompletableFuture<Void> publishAsync(Class<? extends ITopicPublish> topicType, Object message, Executor executor);

    /**
     * Publishes a message asynchronously with additional headers using the supplied executor.
     *
     * @param topicType topic interface type
     * @param message   payload to publish
     * @param headers   additional headers (overrides topic defaults)
     * @param executor  executor to run the publish on
     * @return a future completing when the send is dispatched
     */
    CompletableFuture<Void> publishAsync(Class<? extends ITopicPublish> topicType, Object message, Map<String, Object> headers, Executor executor);

    /**
     * Publishes all messages to the topic in a single call.
     *
     * @param topicType topic interface type
     * @param messages  messages to publish
     */
    void publishBatch(Class<? extends ITopicPublish> topicType, List<?> messages);

    /**
     * Publishes all messages to the topic, chunked into batches of the given size.
     *
     * @param topicType topic interface type
     * @param messages  messages to publish
     * @param batchSize maximum number of messages per chunk
     */
    void publishBatch(Class<? extends ITopicPublish> topicType, List<?> messages, int batchSize);
}
