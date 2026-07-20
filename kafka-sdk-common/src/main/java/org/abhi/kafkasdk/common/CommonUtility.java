package org.abhi.kafkasdk.common;

import io.micrometer.common.util.StringUtils;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;


public class CommonUtility {

    private CommonUtility() {
    }

    public static <V> String convertMessagePayloadToString(V value) {
        if (value instanceof byte[] payloadBytes) {
            return new String(payloadBytes);
        } else {
            return String.valueOf(value);
        }
    }

    public static Map<String, String> convertMessageHeadersToString(MessageHeaders headers) {
        Map<String, String> headersMap = new ConcurrentHashMap<>();
        headers.forEach((key, value) -> headersMap.put(key, String.valueOf(value)));
        return headersMap;
    }

    public static Optional<String> getChannelName(MessageChannel channel) {
        if (channel instanceof AbstractMessageChannel amc) {
            String channelName = amc.getFullChannelName();
            if (StringUtils.isNotEmpty(channelName)) {
                int index = channelName.indexOf(46);
                if (index >= 0) {
                    channelName = channelName.substring(index + 1);
                }
                return Optional.of(channelName);
            }
        }
        return Optional.empty();
    }

    public static Optional<String> getTopicName(BindingProperties bindingProperties) {
        if (bindingProperties != null && bindingProperties.getDestination() != null) {
            return Optional.of(bindingProperties.getDestination());
        }
        return Optional.empty();
    }

    public static Message<?> getSpringMessageWithHeaders(Object message, Map<String, Object> headers) {
        MessageBuilder<?> messageBuilder = MessageBuilder.withPayload(message);
        if (headers != null) {
            headers.forEach(messageBuilder::setHeader);
        }
        return messageBuilder.build();
    }
}
