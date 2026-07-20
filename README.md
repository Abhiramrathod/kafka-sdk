# Kafka SDK

A lightweight Spring Boot SDK that simplifies publishing messages to Apache Kafka. It wraps Spring Cloud Stream's `StreamBridge` behind a clean, interface-driven API — so you define topics as Java interfaces and publish messages with a single method call.

## Why Kafka SDK?

Publishing to Kafka with Spring Cloud Stream requires wiring `StreamBridge`, managing binding names, setting headers, and handling content types manually. This SDK removes that boilerplate:

- **Define topics as interfaces** instead of string-based binding names.
- **Publish with one line** — sync, async, or in batches.
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

## Getting Started

### 1. Add the dependency

Include the starter in your `pom.xml`:

```xml
<dependency>
    <groupId>org.abhi</groupId>
    <artifactId>kafka-sdk-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

This pulls in everything you need — the SDK, auto-configuration, and the Kafka binder.

### 2. Define a topic

Create an interface that extends `ITopicPublish`. Each interface represents a Kafka topic binding:

```java
import org.abhi.kafkasdk.core.ITopicPublish;

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

The `getBinding()` value must match a Spring Cloud Stream binding name defined in your configuration (e.g., `spring.cloud.stream.bindings.orderCreated-out-0.destination`).

### 3. Publish messages

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
        // Synchronous publish
        publishService.publish(OrderCreatedTopic.class, order);
    }
}
```

That's it. The SDK resolves the binding, sets headers, and sends the message through `StreamBridge`.

### 4. Configure bindings

In your `application.yml`:

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

## Building from Source

```bash
git clone https://github.com/abhiram-gh/kafka-sdk.git
cd kafka-sdk
./mvnw clean install
```

## Publishing to Maven Central

This project is configured to publish to Maven Central via Sonatype OSSRH.

### Prerequisites

1. **Sonatype OSSRH Account** — Register at [issues.sonatype.org](https://issues.sonatype.org) and claim the `org.abhi` groupId (requires DNS verification).

2. **GPG Key** — Generate and publish a GPG key:

   ```bash
   # Generate key
   gpg --full-generate-key

   # List keys and copy the KEY_ID
   gpg --list-keys --keyid-format long

   # Send to keyserver
   gpg --keyserver keyserver.ubuntu.com --send-keys KEY_ID
   ```

3. **Maven settings.xml** — Copy `docs/settings-template.xml` to `~/.m2/settings.xml` and fill in:

   ```xml
   <server>
       <id>ossrh</id>
       <username>YOUR_SONATYPE_USERNAME</username>
       <password>YOUR_SONATYPE_PASSWORD</password>
   </server>
   ```

### Release Steps

1. **Remove `-SNAPSHOT` from the version** in the parent `pom.xml`:

   ```xml
   <version>1.0.0</version>
   ```

2. **Build, sign, and deploy to staging**:

   ```bash
   ./mvnw clean deploy -Prelease
   ```

3. **Release from staging**:

   - Log in to [oss.sonatype.org](https://oss.sonatype.org)
   - Go to **Staging Repositories**
   - Select your repository and click **Close**
   - After validation passes, click **Release**

   Or use the nexus-staging plugin:

   ```bash
   ./mvnw nexus-staging:release -Prelease
   ```

4. **Bump to the next SNAPSHOT version** for continued development:

   ```xml
   <version>1.0.1-SNAPSHOT</version>
   ```

### What the Release Profile Activates

| Plugin | Purpose |
|--------|---------|
| `maven-source-plugin` | Generates `-sources.jar` |
| `maven-javadoc-plugin` | Generates `-javadoc.jar` |
| `maven-gpg-plugin` | Signs all artifacts with GPG |

These are only active when you pass `-Prelease`.

### Modules Published

| Artifact | Description |
|----------|-------------|
| `kafka-sdk-core` | Core interfaces |
| `kafka-sdk-common` | Shared utilities |
| `kafka-sdk-service` | PublishService implementation |
| `kafka-sdk-autoconfigure` | Spring Boot auto-configuration |
| `kafka-sdk-starter` | Starter dependency |

## Tech Stack

| Component | Version |
|-----------|---------|
| Java | 17 |
| Spring Boot | 4.1.0 |
| Spring Cloud | 2025.1.2 |
| Apache Kafka | via Spring Cloud Stream Binder |

