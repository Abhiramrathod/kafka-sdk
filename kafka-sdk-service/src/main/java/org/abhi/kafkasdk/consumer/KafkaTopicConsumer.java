package org.abhi.kafkasdk.consumer;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.util.function.Consumer;

/**
 * Functional interface for consuming messages from a Kafka topic through Spring Cloud Stream's
 * functional programming model. It extends {@link Consumer} and can be implemented with a class
 * or a lambda.
 * <p>
 * Register it as a Spring bean; Spring Cloud Stream binds the bean to its input binding
 * (e.g. {@code orderCreatedConsumer-in-0}), matching the binding declared by the bean name.
 *
 * @param <T> payload type of the consumed message
 */
@FunctionalInterface
public interface KafkaTopicConsumer<T> extends Consumer<Message<T>> {

    /**
     * Extracts the deserialized payload from the received message.
     *
     * @param message received message
     * @return payload of the message
     */
    default T getPayload(Message<T> message) {
        return message.getPayload();
    }

    /**
     * Reads a header value and converts it to a string.
     *
     * @param message received message
     * @param name    header name
     * @return header value as a string, or {@code null} when absent
     */
    default String getHeaderAsString(Message<T> message, String name) {
        final Object value = getHeaderValue(message, name);
        return value != null ? String.valueOf(value) : null;
    }

    /**
     * Reads a raw header value.
     *
     * @param message received message
     * @param name    header name
     * @return header value, or {@code null} when absent
     */
    default Object getHeaderValue(Message<T> message, String name) {
        return message.getHeaders().get(name);
    }

    /**
     * Reads the Kafka message key the record was published with.
     *
     * @param message received message
     * @return record key, or {@code null} when absent
     */
    default Object getReceivedKey(Message<T> message) {
        return getHeaderValue(message, KafkaHeaders.RECEIVED_KEY);
    }

    /**
     * Reads the Kafka topic the record was received from.
     *
     * @param message received message
     * @return topic name, or {@code null} when absent
     */
    default String getReceivedTopic(Message<T> message) {
        return getHeaderAsString(message, KafkaHeaders.RECEIVED_TOPIC);
    }

    /**
     * Reads the Kafka partition the record was received from.
     *
     * @param message received message
     * @return partition id, or {@code -1} when absent
     */
    default int getReceivedPartition(Message<T> message) {
        final Object value = getHeaderValue(message, KafkaHeaders.RECEIVED_PARTITION);
        return value instanceof Number number ? number.intValue() : -1;
    }

    /**
     * Acknowledges the message when manual acknowledgement is enabled.
     * <p>
     * This is a no-op when the binding does not use manual acknowledgement
     * (i.e. when no {@link Acknowledgment} is attached to the message).
     *
     * @param message received message
     */
    default void acknowledge(Message<T> message) {
        final Acknowledgment acknowledgment = getAcknowledgement(message);
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    private Acknowledgment getAcknowledgement(Message<T> message) {
        return message.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
    }
}
