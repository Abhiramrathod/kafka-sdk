package org.abhi.kafkasdk.service;

import jakarta.inject.Inject;
import org.abhi.kafkasdk.core.ITopicPublish;
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

import static org.abhi.kafkasdk.common.CommonUtility.getSpringMessageWithHeaders;

public class PublishService implements IPublishService {


    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private StreamBridge streamBridge;


    @Override
    public void publish(Class<? extends ITopicPublish> topicType, Object message) {
        publish(topicType, message, new HashMap<>());
    }

    @Override
    public void publish(Class<? extends ITopicPublish> topicType, Object message, Map<String, Object> headers) {
        ITopicPublish topicPublish = applicationContext.getBean(topicType);

        String binding = topicPublish.getBinding();
        if (binding == null || binding.isEmpty()) {
            throw new IllegalArgumentException("Binding name cannot be null or empty");
        }

        Map<String, Object> mergedHeaders = new HashMap<>(topicPublish.getHeaders());
        mergedHeaders.put(MessageHeaders.CONTENT_TYPE, topicPublish.getContentType());
        mergedHeaders.put("source", applicationContext.getApplicationName());
        mergedHeaders.put("messageId", UUID.randomUUID().toString());
        mergedHeaders.put("timestamp", System.currentTimeMillis());
        mergedHeaders.putAll(headers);

        Message<?> springMessage = getSpringMessageWithHeaders(message, mergedHeaders);

        boolean sent = streamBridge.send(binding, springMessage);
        if (!sent) {
            throw new RuntimeException("Failed to send message to binding: " + binding);
        }
    }

    @Override
    public CompletableFuture<Void> publishAsync(Class<? extends ITopicPublish> topicType, Object message) {
        return publishAsync(topicType, message, new HashMap<>());
    }

    @Override
    public CompletableFuture<Void> publishAsync(Class<? extends ITopicPublish> topicType, Object message, Map<String, Object> headers) {
        return CompletableFuture.runAsync(() -> publish(topicType, message, headers));
    }

    @Override
    public void publishBatch(Class<? extends ITopicPublish> topicType, List<?> messages) {
        publishBatch(topicType, messages, messages.size());
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
            List<?> chunk = messages.subList(i, Math.min(i + batchSize, messages.size()));
            publish(topicType, chunk);
        }
    }
}
