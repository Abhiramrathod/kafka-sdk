package org.abhi.kafkasdk.core.exception;

/**
 * Thrown when a message cannot be published to a Kafka binding.
 */
public class PublishException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public PublishException(String message) {
        super(message);
    }

    public PublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
