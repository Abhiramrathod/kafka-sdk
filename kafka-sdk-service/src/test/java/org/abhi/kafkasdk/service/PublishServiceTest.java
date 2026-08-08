package org.abhi.kafkasdk.service;

import org.abhi.kafkasdk.core.ITopicPublish;
import org.abhi.kafkasdk.core.exception.PublishException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PublishServiceTest {

    private static final String BINDING = "orderCreated-out-0";

    interface OrderCreatedTopic extends ITopicPublish {
        @Override
        default String getBinding() {
            return BINDING;
        }
    }

    interface MissingBindingTopic extends ITopicPublish {
    }

    private ApplicationContext applicationContext;
    private StreamBridge streamBridge;
    private PublishService publishService;

    @BeforeEach
    void setUp() {
        applicationContext = mock(ApplicationContext.class);
        streamBridge = mock(StreamBridge.class);
        publishService = new PublishService(applicationContext, streamBridge);
        when(applicationContext.getApplicationName()).thenReturn("test-app");
        when(applicationContext.getBean(OrderCreatedTopic.class))
                .thenReturn(mock(OrderCreatedTopic.class, CALLS_REAL_METHODS));
        when(applicationContext.getBean(MissingBindingTopic.class))
                .thenReturn(mock(MissingBindingTopic.class, CALLS_REAL_METHODS));
    }

    @Test
    void publishSendsMessageWithDefaultHeaders() {
        when(streamBridge.send(eq(BINDING), any(Message.class))).thenReturn(true);

        publishService.publish(OrderCreatedTopic.class, "payload");

        Message<?> sent = captureSentMessage();
        assertEquals("payload", sent.getPayload());
        MessageHeaders headers = sent.getHeaders();
        assertEquals("application/json", headers.get(MessageHeaders.CONTENT_TYPE));
        assertEquals("test-app", headers.get("source"));
        assertNotNull(headers.get("messageId"));
        assertNotNull(headers.get("timestamp"));
    }

    @Test
    void publishMergesCustomHeadersOverDefaults() {
        when(streamBridge.send(eq(BINDING), any(Message.class))).thenReturn(true);

        publishService.publish(OrderCreatedTopic.class, "payload", Map.of("correlationId", "abc-123"));

        Message<?> sent = captureSentMessage();
        assertEquals("abc-123", sent.getHeaders().get("correlationId"));
    }

    @Test
    void publishThrowsWhenBindingIsMissing() {
        assertThrows(IllegalArgumentException.class,
                () -> publishService.publish(MissingBindingTopic.class, "payload"));
    }

    @Test
    void publishThrowsPublishExceptionWhenSendFails() {
        when(streamBridge.send(eq(BINDING), any(Message.class))).thenReturn(false);

        assertThrows(PublishException.class,
                () -> publishService.publish(OrderCreatedTopic.class, "payload"));
    }

    @Test
    void publishThrowsWhenTopicTypeIsNull() {
        assertThrows(IllegalArgumentException.class, () -> publishService.publish(null, "payload"));
    }

    @Test
    void publishCachesTopicLookup() {
        when(streamBridge.send(eq(BINDING), any(Message.class))).thenReturn(true);

        publishService.publish(OrderCreatedTopic.class, "a");
        publishService.publish(OrderCreatedTopic.class, "b");

        verify(applicationContext, times(1)).getBean(OrderCreatedTopic.class);
    }

    @Test
    void publishBatchWithNullListDoesNothing() {
        publishService.publishBatch(OrderCreatedTopic.class, null);
        verify(streamBridge, never()).send(anyString(), any(Message.class));
    }

    @Test
    void publishBatchWithEmptyListDoesNothing() {
        publishService.publishBatch(OrderCreatedTopic.class, List.of());
        verify(streamBridge, never()).send(anyString(), any(Message.class));
    }

    @Test
    void publishBatchChunksMessagesBySize() {
        when(streamBridge.send(eq(BINDING), any(Message.class))).thenReturn(true);

        publishService.publishBatch(OrderCreatedTopic.class, List.of("1", "2", "3", "4", "5"), 2);

        verify(streamBridge, times(3)).send(eq(BINDING), any(Message.class));
    }

    @Test
    void publishBatchWithoutSizePublishesAllAtOnce() {
        when(streamBridge.send(eq(BINDING), any(Message.class))).thenReturn(true);

        publishService.publishBatch(OrderCreatedTopic.class, List.of("1", "2", "3"));

        verify(streamBridge, times(1)).send(eq(BINDING), any(Message.class));
    }

    @Test
    void publishBatchThrowsOnInvalidBatchSize() {
        assertThrows(IllegalArgumentException.class,
                () -> publishService.publishBatch(OrderCreatedTopic.class, List.of("1"), 0));
    }

    @Test
    void publishAsyncCompletesAndSends() throws Exception {
        when(streamBridge.send(eq(BINDING), any(Message.class))).thenReturn(true);

        CompletableFuture<Void> future = publishService.publishAsync(OrderCreatedTopic.class, "payload");

        future.get(5, TimeUnit.SECONDS);
        verify(streamBridge).send(eq(BINDING), any(Message.class));
    }

    @Test
    void publishAsyncWithCustomExecutorUsesIt() throws Exception {
        when(streamBridge.send(eq(BINDING), any(Message.class))).thenReturn(true);
        Executor executor = Executors.newSingleThreadExecutor();
        AtomicInteger invocations = new AtomicInteger();

        publishService.publishAsync(OrderCreatedTopic.class, "payload", task -> {
            invocations.incrementAndGet();
            task.run();
        }).get(5, TimeUnit.SECONDS);

        assertEquals(1, invocations.get());
        verify(streamBridge).send(eq(BINDING), any(Message.class));
    }

    private Message<?> captureSentMessage() {
        org.mockito.ArgumentCaptor<Message<?>> captor = org.mockito.ArgumentCaptor.forClass(Message.class);
        verify(streamBridge).send(eq(BINDING), captor.capture());
        return captor.getValue();
    }
}
