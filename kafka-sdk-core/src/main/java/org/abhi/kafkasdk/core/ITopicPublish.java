package org.abhi.kafkasdk.core;

import java.util.Collections;
import java.util.Map;

/**
 * Describes a Kafka topic that the SDK can publish to.
 * <p>
 * Implementations are plain interfaces; the SDK resolves them as Spring beans,
 * so annotate the interface with a Spring stereotype (e.g. {@code @Component})
 * or expose it via a configuration class.
 */
public interface ITopicPublish {

    /**
     * Spring Cloud Stream binding name (e.g. {@code orderCreated-out-0}).
     *
     * @return the binding name
     */
    default String getBinding() {
        return null;
    }

    /**
     * Default headers attached to every message sent to this topic.
     *
     * @return map of header values
     */
    default Map<String, Object> getHeaders() {
        return Collections.emptyMap();
    }

    /**
     * Content type of the published payload.
     *
     * @return content type, defaults to {@code application/json}
     */
    default String getContentType() {
        return "application/json";
    }
}
