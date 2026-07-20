package org.abhi.kafkasdk.core.service;

import org.abhi.kafkasdk.core.ITopicPublish;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface IPublishService {

    void publish(Class<? extends ITopicPublish> topicType, Object message);

    void publish(Class<? extends ITopicPublish> topicType, Object message, Map<String, Object> headers);

    CompletableFuture<Void> publishAsync(Class<? extends ITopicPublish> topicType, Object message);

    CompletableFuture<Void> publishAsync(Class<? extends ITopicPublish> topicType, Object message, Map<String, Object> headers);

    void publishBatch(Class<? extends ITopicPublish> topicType, List<?> messages);

    void publishBatch(Class<? extends ITopicPublish> topicType, List<?> messages, int batchSize);
}
