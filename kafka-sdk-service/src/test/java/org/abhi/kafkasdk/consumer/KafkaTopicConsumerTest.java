package org.abhi.kafkasdk.consumer;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class KafkaTopicConsumerTest {

    @Test
    void acceptExposesPayloadAndKafkaMetadataHeaders() {
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        Message<String> message = new GenericMessage<>("hello", new MessageHeaders(Map.of(
                KafkaHeaders.RECEIVED_KEY, "user-1",
                KafkaHeaders.RECEIVED_TOPIC, "orders",
                KafkaHeaders.RECEIVED_PARTITION, 3,
                KafkaHeaders.ACKNOWLEDGMENT, acknowledgment)));

        RecordingConsumer consumer = new RecordingConsumer();
        consumer.accept(message);

        assertEquals("hello", consumer.payload);
        assertEquals("user-1", consumer.key);
        assertEquals("orders", consumer.topic);
        assertEquals(3, consumer.partition);
        assertEquals(acknowledgment, consumer.acknowledgment);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void acknowledgeIsNoOpWithoutHeader() {
        Message<String> message = new GenericMessage<>("hello", new MessageHeaders(Map.of()));

        RecordingConsumer consumer = new RecordingConsumer();
        consumer.accept(message);

        assertNull(consumer.key);
        assertNull(consumer.topic);
        assertEquals(-1, consumer.partition);
        assertNull(consumer.acknowledgment);
    }

    static class RecordingConsumer extends KafkaTopicConsumer<String> {

        String payload;
        String key;
        String topic;
        int partition = -1;
        Acknowledgment acknowledgment;

        @Override
        public void accept(Message<String> message) {
            this.payload = getPayload(message);
            this.key = getHeaderAsString(message, KafkaHeaders.RECEIVED_KEY);
            this.topic = getReceivedTopic(message);
            this.partition = getReceivedPartition(message);
            this.acknowledgment = (Acknowledgment) getHeaderValue(message, KafkaHeaders.ACKNOWLEDGMENT);
            acknowledge(message);
        }
    }
}
