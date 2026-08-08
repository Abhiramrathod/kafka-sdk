package org.abhi.kafkasdk.common;

import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CommonUtilityTest {

    @Test
    void getSpringMessageWithHeadersBuildsMessage() {
        Message<?> message = CommonUtility.getSpringMessageWithHeaders("payload", Map.of("key", "value"));

        assertEquals("payload", message.getPayload());
        assertEquals("value", message.getHeaders().get("key"));
    }

    @Test
    void getSpringMessageWithHeadersHandlesNullHeaders() {
        Message<?> message = CommonUtility.getSpringMessageWithHeaders("payload", null);

        assertEquals("payload", message.getPayload());
    }

    @Test
    void convertMessagePayloadToStringFromBytes() {
        assertEquals("hello", CommonUtility.convertMessagePayloadToString("hello".getBytes()));
    }

    @Test
    void convertMessagePayloadToStringFromObject() {
        assertEquals("123", CommonUtility.convertMessagePayloadToString(123));
    }

    @Test
    void convertMessageHeadersToStringConvertsValues() {
        Message<?> message = MessageBuilder.withPayload("p").setHeader("a", 1).build();

        Map<String, String> converted = CommonUtility.convertMessageHeadersToString(message.getHeaders());

        assertEquals("1", converted.get("a"));
    }

    @Test
    void getTopicNameReturnsDestination() {
        BindingProperties properties = new BindingProperties();
        properties.setDestination("orders");

        assertEquals("orders", CommonUtility.getTopicName(properties).orElse(""));
    }

    @Test
    void getTopicNameEmptyWhenNoDestination() {
        assertTrue(CommonUtility.getTopicName(new BindingProperties()).isEmpty());
    }

    @Test
    void getChannelNameExtractsSuffixAfterDot() {
        AbstractMessageChannel channel = mock(AbstractMessageChannel.class);
        when(channel.getFullChannelName()).thenReturn(
                "org.springframework.integration.channel.DirectChannel.orderCreated-out-0");

        assertEquals("orderCreated-out-0", CommonUtility.getChannelName(channel).orElse(""));
    }

    @Test
    void getChannelNameEmptyForUnknownChannel() {
        MessageChannel channel = mock(MessageChannel.class);

        assertTrue(CommonUtility.getChannelName(channel).isEmpty());
    }
}
