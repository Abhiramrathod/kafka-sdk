package org.abhi.kafkasdk.common;

import io.micrometer.common.util.StringUtils;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared utilities for message conversion, header handling, and channel resolution.
 */
public final class CommonUtility {

    private CommonUtility() {
    }

    public static <V> String convertMessagePayloadToString(V value) {
        final String result;
        if (value instanceof byte[] payloadBytes) {
            result = new String(payloadBytes);
        } else {
            result = String.valueOf(value);
        }
        return result;
    }

    public static Map<String, String> convertMessageHeadersToString(MessageHeaders headers) {
        final Map<String, String> headersMap = new ConcurrentHashMap<>();
        headers.forEach((key, value) -> headersMap.put(key, String.valueOf(value)));
        return headersMap;
    }

    public static Optional<String> getChannelName(MessageChannel channel) {
        final Optional<String> channelName;
        if (channel instanceof AbstractMessageChannel amc) {
            String name = amc.getFullChannelName();
            if (StringUtils.isNotEmpty(name)) {
                final int index = name.lastIndexOf('.');
                if (index >= 0) {
                    name = name.substring(index + 1);
                }
                channelName = Optional.of(name);
            } else {
                channelName = Optional.empty();
            }
        } else {
            channelName = Optional.empty();
        }
        return channelName;
    }

    public static Optional<String> getTopicName(BindingProperties bindingProperties) {
        final Optional<String> topicName;
        if (bindingProperties != null && bindingProperties.getDestination() != null) {
            topicName = Optional.of(bindingProperties.getDestination());
        } else {
            topicName = Optional.empty();
        }
        return topicName;
    }

    public static Message<?> getSpringMessageWithHeaders(Object message, Map<String, Object> headers) {
        final Map<String, Object> resolvedHeaders = headers == null ? Map.of() : headers;
        return new GenericMessage<>(message, new MessageHeaders(resolvedHeaders));
    }
}
