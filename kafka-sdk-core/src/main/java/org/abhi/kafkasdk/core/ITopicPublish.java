package org.abhi.kafkasdk.core;

import java.util.Collections;
import java.util.Map;

public interface ITopicPublish {

    default String getBinding() {
        return null;
    }

    default Map<String, Object> getHeaders() {
        return Collections.emptyMap();
    }

    default String getContentType() {
        return "application/json";
    }
}
