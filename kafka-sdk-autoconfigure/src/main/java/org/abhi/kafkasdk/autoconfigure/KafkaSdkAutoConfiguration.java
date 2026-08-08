package org.abhi.kafkasdk.autoconfigure;

import org.abhi.kafkasdk.service.PublishService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@ConditionalOnProperty(name = "kafka.sdk.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaSdkAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public PublishService publishService(ApplicationContext context, StreamBridge streamBridge) {
        return new PublishService(context, streamBridge);
    }
}
