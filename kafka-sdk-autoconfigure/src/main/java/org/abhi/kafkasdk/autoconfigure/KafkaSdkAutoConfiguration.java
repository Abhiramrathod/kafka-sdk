package org.abhi.kafkasdk.autoconfigure;

import org.abhi.kafkasdk.service.PublishService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
public class KafkaSdkAutoConfiguration {

    @Bean
    public PublishService publishService() {
        return new PublishService();
    }
}
