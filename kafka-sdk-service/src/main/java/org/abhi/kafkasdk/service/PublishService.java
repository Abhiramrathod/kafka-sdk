package org.abhi.kafkasdk.service;

import org.abhi.kafkasdk.core.ITopicPublish;
import org.abhi.kafkasdk.core.exception.PublishException;
import org.abhi.kafkasdk.core.service.IPublishService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static org.abhi.kafkasdk.common.CommonUtility.getSpringMessageWithHeaders;

/**
 * Default {@link IPublishService} implementation backed by Spring Cloud Stream's {@link StreamBridge}.
 */
public class PublishService implements IPublishService {

    private final ApplicationContext context;
    private final StreamBridge streamBridge;
    private final Map<Class<? extends ITopicPublish>, ITopicPublish> topicCache = new ConcurrentHashMap<>();

    public PublishService(ApplicationContext context, StreamBridge streamBridge) {
        this.context = context;
        this.streamBridge = streamBridge;
    }

    @Override
    public void publish(Class<? extends ITopicPublish> topicType, Object message) {
        publish(topicType, message, new HashMap<>());
    }

    @Override
    public void publish(Class<? extends ITopicPublish> topicType, Object message, Map<String, Object> headers) {
        final ITopicPublish topicPublish = resolveTopic(topicType);

        final String binding = topicPublish.getBinding();
        if (binding == null || binding.isEmpty()) {
            throw new IllegalArgumentException("Binding name cannot be null or empty for topic: " + topicType.getName());
        }

        final Map<String, Object> mergedHeaders = buildHeaders(topicPublish, headers);

        final Message<?> springMessage = getSpringMessageWithHeaders(message, mergedHeaders);

        final boolean sent = streamBridge.send(binding, springMessage);
        if (!sent) {
            throw new PublishException("Failed to send message to binding: " + binding);
        }
    }

    @Override
    public CompletableFuture<Void> publishAsync(Class<? extends ITopicPublish> topicType, Object message) {
        return publishAsync(topicType, message, new HashMap<>(), null);
    }

    @Override
    public CompletableFuture<Void> publishAsync(Class<? extends ITopicPublish> topicType, Object message, Map<String, Object> headers) {
        return publishAsync(topicType, message, headers, null);
    }

    @Override
    public CompletableFuture<Void> publishAsync(Class<? extends ITopicPublish> topicType, Object message, Executor executor) {
        return publishAsync(topicType, message, new HashMap<>(), executor);
    }

    @Override
    public CompletableFuture<Void> publishAsync(Class<? extends ITopicPublish> topicType, Object message, Map<String, Object> headers, Executor executor) {
        final Runnable task = () -> publish(topicType, message, headers);
        return executor != null
                ? CompletableFuture.runAsync(task, executor)
                : CompletableFuture.runAsync(task);
    }

    @Override
    public void publishBatch(Class<? extends ITopicPublish> topicType, List<?> messages) {
        publishBatch(topicType, messages, messages == null ? 0 : messages.size());
    }

    @Override
    public void publishBatch(Class<? extends ITopicPublish> topicType, List<?> messages, int batchSize) {
        if (messages == null || messages.isEmpty()) {
            return;
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be greater than 0");
        }
        for (int i = 0; i < messages.size(); i += batchSize) {
            final List<?> chunk = messages.subList(i, Math.min(i + batchSize, messages.size()));
            publish(topicType, chunk);
        }
    }

    private ITopicPublish resolveTopic(Class<? extends ITopicPublish> topicType) {
        if (topicType == null) {
            throw new IllegalArgumentException("Topic type cannot be null");
        }
        return topicCache.computeIfAbsent(topicType, context::getBean);
    }

    private Map<String, Object> buildHeaders(ITopicPublish topicPublish, Map<String, Object> headers) {
        final Map<String, Object> mergedHeaders = new HashMap<>(topicPublish.getHeaders());
        mergedHeaders.put(MessageHeaders.CONTENT_TYPE, topicPublish.getContentType());
        mergedHeaders.put("source", context.getApplicationName());
        mergedHeaders.put("messageId", UUID.randomUUID().toString());
        mergedHeaders.put("timestamp", System.currentTimeMillis());
        if (headers != null) {
            mergedHeaders.putAll(headers);
        }
        return mergedHeaders;
    }
}
