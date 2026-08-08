# Kafka SDK

A lightweight Spring Boot SDK that simplifies publishing messages to Apache Kafka. It wraps Spring Cloud Stream's `StreamBridge` behind a clean, interface-driven API — so you define topics as Java interfaces and publish messages with a single method call.

[![Build & Release](https://github.com/Abhiramrathod/kafka-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/Abhiramrathod/kafka-sdk/actions/workflows/ci.yml)
[![](https://jitpack.io/v/Abhiramrathod/kafka-sdk.svg)](https://jitpack.io/#Abhiramrathod/kafka-sdk)

## Why Kafka SDK?

Publishing to Kafka with Spring Cloud Stream requires wiring `StreamBridge`, managing binding names, setting headers, and handling content types manually. This SDK removes that boilerplate:

- **Define topics as interfaces** instead of string-based binding names.
- **Publish with one line** — sync, async, or in batches.
- **Consume with a plain class** — extend a base consumer to read records, Kafka metadata, and manual acknowledgements.
- **Auto-configured** — add the starter dependency and it works.
- **Header management built in** — `messageId`, `timestamp`, `source`, and `contentType` are set automatically.

## Modules

| Module | Artifact | Purpose |
|--------|----------|---------|
| **Core** | `kafka-sdk-core` | Defines the `ITopicPublish` interface and `IPublishService` contract. |
| **Common** | `kafka-sdk-common` | Shared utilities for message conversion and header handling. |
| **Service** | `kafka-sdk-service` | Implements `IPublishService` using `StreamBridge`. |
| **Autoconfigure** | `kafka-sdk-autoconfigure` | Registers `PublishService` as a Spring bean automatically. |
| **Starter** | `kafka-sdk-starter` | Bundles autoconfigure + Kafka binder into a single dependency. |

## Requirements

- Java 17+
- Spring Boot 4.1+
- Spring Cloud 2025.1+
- Apache Kafka

## Installation

### Maven

Add the JitPack repository and the dependency to your `pom.xml`:

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.Abhiramrathod</groupId>
    <artifactId>kafka-sdk-starter</artifactId>
    <version>1.0.1</version>
</dependency>
```

### Gradle

```groovy
repositories {
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.Abhiramrathod:kafka-sdk-starter:1.0.1'
}
```

Check [jitpack.io/#Abhiramrathod/kafka-sdk](https://jitpack.io/#Abhiramrathod/kafka-sdk) for available versions.

## Quick Start

### 1. Define a topic

Create an interface that extends `ITopicPublish` and register it as a Spring bean:

```java
import org.abhi.kafkasdk.core.ITopicPublish;
import org.springframework.stereotype.Component;

@Component
public interface OrderCreatedTopic extends ITopicPublish {

    @Override
    default String getBinding() {
        return "orderCreated-out-0";
    }

    @Override
    default String getContentType() {
        return "application/json";
    }
}
```

The SDK resolves the topic interface through the Spring context, so it must be a bean (the interface itself, or a `@Configuration` class exposing it via `@Bean`).

### 2. Publish messages

Inject `IPublishService` and publish using the topic interface:

```java
import org.abhi.kafkasdk.core.service.IPublishService;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final IPublishService publishService;

    public OrderService(IPublishService publishService) {
        this.publishService = publishService;
    }

    public void createOrder(Order order) {
        publishService.publish(OrderCreatedTopic.class, order);
    }
}
```

### 3. Configure bindings

```yaml
spring:
  cloud:
    stream:
      kafka:
        binder:
          brokers: localhost:9092
      bindings:
        orderCreated-out-0:
          destination: order-created-topic
          content-type: application/json
```

## Usage

### Sync publish

```java
publishService.publish(OrderCreatedTopic.class, order);
```

### Sync publish with custom headers

```java
Map<String, Object> headers = Map.of("correlationId", "abc-123");
publishService.publish(OrderCreatedTopic.class, order, headers);
```

### Async publish

Returns a `CompletableFuture<Void>`:

```java
publishService.publishAsync(OrderCreatedTopic.class, order)
    .thenRun(() -> log.info("Message sent"));
```

### Batch publish

Sends a list of messages, optionally chunked into batches:

```java
List<Order> orders = List.of(order1, order2, order3);

// Send all at once
publishService.publishBatch(OrderCreatedTopic.class, orders);

// Send in chunks of 10
publishService.publishBatch(OrderCreatedTopic.class, orders, 10);
```

## Consume messages

### 1. Define a consumer

Extend `KafkaTopicConsumer<T>` and implement `accept(Message<T>)`:

```java
import org.abhi.kafkasdk.consumer.KafkaTopicConsumer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedConsumer extends KafkaTopicConsumer<Order> {

    @Override
    public void accept(Message<Order> message) {
        Order order = getPayload(message);
        String key = getHeaderAsString(message, KafkaHeaders.RECEIVED_MESSAGE_KEY);
        System.out.println("Order " + order.getId() + " (key: " + key + ")");
    }
}
```

Spring Cloud Stream binds the consumer bean to its input binding. The binding name is derived from the bean name — for `orderCreatedConsumer` it becomes `orderCreatedConsumer-in-0`.

### 2. Configure the binding

```yaml
spring:
  cloud:
    stream:
      bindings:
        orderCreatedConsumer-in-0:
          destination: order-created-topic
          group: order-service
```

### Reading Kafka metadata

The base class exposes protected helpers for record metadata, so you never touch raw headers:

| Helper | Returns |
|--------|---------|
| `getPayload(message)` | Deserialized payload |
| `getReceivedTopic(message)` | Topic the record came from |
| `getReceivedPartition(message)` | Partition id (or `-1` if absent) |
| `getReceivedKey(message)` | Record key |
| `getHeaderValue(message, name)` | Raw header value |
| `getHeaderAsString(message, name)` | Header value as a string |

### Manual acknowledgement

When a consumer group uses manual acknowledgement, call `acknowledge(message)` after the record is fully processed:

```yaml
spring:
  cloud:
    stream:
      bindings:
        orderCreatedConsumer-in-0:
          destination: order-created-topic
          group: order-service
          consumer:
            acknowledgeMode: MANUAL
```

```java
@Override
public void accept(Message<Order> message) {
    // process order...
    acknowledge(message);
}
```

`acknowledge` is a no-op when the binding does not use manual acknowledgement.

## Automatic Headers

The SDK attaches these headers to every message automatically:

| Header | Value |
|--------|-------|
| `contentType` | From `ITopicPublish.getContentType()` (default: `application/json`) |
| `source` | Application name (`spring.application.name`) |
| `messageId` | UUID generated per message |
| `timestamp` | Current epoch millis |

You can also define default headers per topic:

```java
public interface AuditTopic extends ITopicPublish {

    @Override
    default String getBinding() {
        return "audit-out-0";
    }

    @Override
    default Map<String, Object> getHeaders() {
        return Map.of("category", "audit");
    }
}
```

## Tech Stack

| Component | Version |
|-----------|---------|
| Java | 17 |
| Spring Boot | 4.1.0 |
| Spring Cloud | 2025.1.2 |
| Apache Kafka | via Spring Cloud Stream Binder |
