# Product Engine

Configuration-driven Solace message processing engine built with Java, Spring Boot, and JCSMP.

This README captures the **current implementation as of the current development stage**. The current design is intentionally frozen before introducing the next set of features.

## 1. Technology Stack

- Java 26 runtime
- Spring Boot 3.5.16
- Solace JCSMP 10.28.1
- Maven
- Spring Boot Actuator
- Lombok
- Jackson
- Guava

## 2. Current Architecture

Each configured product has one Solace durable queue, one JCSMP `FlowReceiver`, N application workers, and its own destination client.

```text
                         application.yml
                               |
                               v
                    Product Engine Properties
                               |
              +----------------+----------------+
              |                |                |
              v                v                v
          PRODUCT_1        PRODUCT_2        PRODUCT_N
              |                |                |
              v                v                v
        FlowReceiver      FlowReceiver      FlowReceiver
              |                |                |
       +------+------+   +-----+-----+   +-----+-----+
       |      |      |   |     |     |   |           |
       v      v      v   v     v     v   v           v
      W1     W2     W3  W1    W2    W3  W1         WN
       |      |      |   |     |     |
       +------+------+   +-----+-----+
              |                |
              v                v
        Destination       Destination
```

### Core design decision

**One FlowReceiver for N workers for one product.**

Workers synchronously call `receive()` on the shared `FlowReceiver`.

There is currently **no application-owned message queue** between the receiver and the workers.

## 3. Product Configuration

Current configuration structure:

```yaml
product-engine:
  products:

    PRODUCT_1:
      queue: QUEUE_BULK_POLL_PRODUCT1_TEST

      consumer:
        concurrency: 5
        max-unacked-messages: 5

        rate-limit:
          enabled: true
          tps: 50

      destination:
        type: MOCK
        processing-time-ms: 500

        rate-limit:
          enabled: true
          tps: 20

    PRODUCT_2:
      queue: QUEUE_BULK_POLL_PRODUCT2_TEST

      consumer:
        concurrency: 10
        max-unacked-messages: 10

        rate-limit:
          enabled: true
          tps: 100

      destination:
        type: MOCK
        processing-time-ms: 200

        rate-limit:
          enabled: true
          tps: 50
```

| Property | Current purpose/status |
|---|---|
| `queue` | Solace durable queue associated with the product |
| `consumer.concurrency` | Number of application worker threads |
| `consumer.max-unacked-messages` | Present in configuration; broker-side unacked-message control is intentionally not being used as the application control mechanism |
| `consumer.rate-limit.enabled` | Enables application-side consumption throttling |
| `consumer.rate-limit.tps` | Target application consumption rate before `receive()` |
| `destination.rate-limit.enabled` | Enables application-side destination throttling |
| `destination.rate-limit.tps` | Target rate for downstream destination admission |
| `destination.type` | Destination implementation type |
| `destination.processing-time-ms` | Processing delay for the current mock destination |

## 4. Solace Connection

One JCSMP session is created by `JcsmpSessionManager` and shared by the product flows.

```text
JcsmpSessionManager
        |
        v
   JCSMPSession
        |
        +---- PRODUCT_1 FlowReceiver
        +---- PRODUCT_2 FlowReceiver
        +---- PRODUCT_N FlowReceiver
```

`JcsmpSessionManager` is responsible for creating, connecting, and closing the JCSMP session.

Current local test connection:

```yaml
solace:
  jcsmp:
    host: tcp://localhost:55555
    vpn: TEST_SHAIK_BULK_VPN
    username: admin
    password: admin
```

## 5. Message Consumption

For each product:

1. Create the configured Solace queue.
2. Create one `FlowReceiver`.
3. Start the flow.
4. Start N application workers.
5. Each worker calls `flow.receive(1000)`.
6. The worker processes the message.
7. The worker acknowledges the message after successful processing.
8. The worker receives the next message.

```text
Solace Durable Queue
        |
        v
   FlowReceiver
        |
   +----+----+----+----+
   |    |    |    |    |
   v    v    v    v    v
  W1   W2   W3   W4   W5
   |    |    |    |    |
 receive() on shared FlowReceiver
   |    |    |    |    |
 process message
   |    |    |    |    |
 ACK after successful processing
```

## 6. Application Rate Limiting

Rate limiting is now part of the current architecture. There are two independent application-side controls per product.

```text
Solace Queue
    |
    v
Consumption RateLimiter
    |
    v
flow.receive()
    |
    v
N Workers
    |
    v
Destination RateLimiter
    |
    v
Destination
    |
    v
ACK
```

### Consumption rate limiter

The consumption limiter is acquired **before** `flow.receive()`. It controls how quickly the application admits messages from Solace.

### Destination rate limiter

The destination limiter is acquired after a message is received and before the downstream destination call. It controls how quickly the application invokes the destination.

### Concurrency vs rate limiting

These are separate controls:

- **Concurrency** = number of messages processed in parallel.
- **Consumption TPS** = how quickly the application consumes messages.
- **Destination TPS** = how quickly downstream calls are admitted.

A simplified effective-throughput relationship is:

```text
Effective TPS ≈ min(
    Consumption TPS,
    Destination TPS,
    Concurrency / Processing Latency
)
```

The current implementation uses **Guava `RateLimiter`** for conceptual clarity. The limiter is abstracted so that it can later be tuned/replaced with Bucket4j without changing the overall processing architecture.

No application-owned queue has been introduced just to support rate limiting. Workers may block while waiting for a rate-limit permit.

## 6. Application Worker Model

Each `ProductRuntime` creates a fixed-size worker pool based on:

```yaml
consumer:
  concurrency: N
```

The worker lifecycle is:

```text
consumption rate limiter
   |
   v
receive()
   |
   v
deserialize
   |
   v
destination rate limiter
   |
   v
destination processing
   |
   v
success?
   |
  YES
   |
   v
ackMessage()
   |
   v
receive next message
```

The worker itself performs the receive operation. This avoids the previous architecture where a JCSMP callback submitted every message into another executor.

## 7. Why There Is No Application Message Queue

The current design intentionally does not introduce an additional application-owned message queue.

```text
Solace Queue
     |
     v
FlowReceiver
     |
     +--> Worker
     +--> Worker
     +--> Worker
     +--> Worker
```

The earlier callback-to-executor model could reject message submissions when all workers were busy. The current synchronous-worker model removes that executor submission path.

## 8. Message Acknowledgement

Client acknowledgement is enabled.

A message is acknowledged only after downstream processing succeeds:

```java
process(message, workerName);
message.ackMessage();
```

Therefore:

```text
Receive
   |
   v
Process
   |
   +---- failure ---> no ACK
   |
   v
 ACK
```

Detailed retry, redelivery, and DMQ behavior are **not finalized yet**.

## 9. Product Runtime

`ProductRuntime` currently owns:

- Worker threads
- Message receiving
- Payload conversion
- Destination invocation
- Successful-message acknowledgement
- Graceful worker shutdown

`ProductRuntimeRegistry` maintains the configured product runtimes.

```text
ProductRuntimeRegistry
        |
        +---- ProductRuntime PRODUCT_1
        |          |
        |          +---- Workers
        |
        +---- ProductRuntime PRODUCT_2
                   |
                   +---- Workers
```

Product runtimes are isolated from each other.

## 10. Destination

The current destination implementation is a mock destination.

It can simulate downstream processing latency:

```yaml
destination:
  type: MOCK
  processing-time-ms: 500
```

This allows worker concurrency and processing behavior to be tested without a real downstream system.

## 11. Graceful Shutdown

Graceful shutdown has been implemented and verified.

There are two separate `@PreDestroy` responsibilities.

### `JcsmpProductConsumer`

Responsible for stopping:

1. Product runtimes/workers
2. Product `FlowReceiver`s

```text
JcsmpProductConsumer @PreDestroy
        |
        v
ProductRuntimeRegistry.shutdown()
        |
        v
Stop workers
        |
        v
Wait for workers
        |
        v
Stop / close FlowReceivers
```

### `JcsmpSessionManager`

Responsible for:

```text
JcsmpSessionManager @PreDestroy
        |
        v
Close JCSMPSession
```

Overall shutdown order:

```text
Application Shutdown
        |
        v
Stop Product Workers
        |
        v
Stop / Close FlowReceivers
        |
        v
Close JCSMP Session
```

The worker currently uses:

```java
flow.receive(1000);
```

Therefore a worker waiting for a message may take approximately one second to exit during shutdown.

## 12. Current Shutdown Verification

The shutdown sequence has been tested successfully.

Observed sequence:

```text
Stopping JCSMP product consumers
        |
        v
Stopping PRODUCT_2 runtime
        |
        v
PRODUCT_2 workers stopped
        |
        v
Stopping PRODUCT_1 runtime
        |
        v
PRODUCT_1 workers stopped
        |
        v
PRODUCT_2 Flow stopped
        |
        v
PRODUCT_1 Flow stopped
        |
        v
JCSMP product consumers stopped
        |
        v
JCSMP session closed
```

No `receive()` / closed-flow exceptions were observed in the successful shutdown sequence.

## 13. Current Test Publisher

A temporary REST-based JCSMP publisher is available for local testing.

Endpoint:

```text
POST /test/publish/{product}
```

Example:

```powershell
Invoke-RestMethod `
  -Uri "http://localhost:9177/test/publish/PRODUCT_1" `
  -Method POST `
  -ContentType "application/json" `
  -Body '{"count":5,"requestType":"TEST","payload":"Hello PRODUCT_1"}'
```

The publisher is currently intended only for local development/testing.

## 14. Current Local Test Environment

```text
Solace Host : localhost
Port        : 55555
VPN         : TEST_SHAIK_BULK_VPN
Username    : admin
```

Configured test queues:

```text
QUEUE_BULK_POLL_PRODUCT1_TEST
QUEUE_BULK_POLL_PRODUCT2_TEST
```

Application port:

```text
9177
```

## 15. Current Project Components

```text
config/
├── ProductConfig
├── ProductEngineProperties
└── JcsmpProperties

jcsmp/
├── JcsmpSessionManager
└── JcsmpProductConsumer

runtime/
├── ProductRuntime
├── ProductRuntimeRegistry
└── ProductMessageConverter

destination/
├── DestinationClient
└── MockDestinationClient
```

## 16. Current Implementation Status

### Implemented and Frozen

- [x] Spring Boot application
- [x] Configuration-driven product definitions
- [x] Product runtime registry
- [x] One durable queue per product
- [x] One JCSMP `FlowReceiver` per product
- [x] N application workers per product
- [x] Shared `FlowReceiver` across product workers
- [x] Synchronous `receive()` model
- [x] Payload conversion
- [x] Mock destination
- [x] Downstream processing
- [x] Client acknowledgement after successful processing
- [x] Product-level runtime isolation
- [x] Graceful worker shutdown
- [x] Graceful FlowReceiver shutdown
- [x] Graceful JCSMP session shutdown
- [x] Local REST test publisher
- [x] Basic Actuator endpoints

### Not Yet Implemented / Finalized

- [ ] Production tuning/replacement of Guava rate limiter (Bucket4j evaluation)
- [ ] Distributed/global rate-limit semantics across multiple application instances
- [ ] Retry policy
- [ ] Redelivery strategy
- [ ] Failure classification
- [ ] DMQ strategy
- [ ] Ordering guarantees
- [ ] Production destination implementations
- [ ] Runtime configuration refresh
- [ ] Metrics design
- [ ] Detailed observability
- [ ] Production health checks
- [ ] Advanced backpressure/admission control
- [ ] Retry/redelivery/DMQ behavior
- [ ] Configuration validation
- [ ] Dynamic product onboarding/removal

## 17. Validation and Test Findings

The rate-limiting design has been validated with local Solace tests using a mock destination.

### Test observations

With approximately 500 ms destination processing latency and 5 workers:

```text
Worker capacity ≈ 5 / 0.5
                ≈ 10 TPS
```

Observed behavior across tests:

| Consumption limit | Approx. observed throughput | Interpretation |
|---:|---:|---|
| 5 TPS | ~5 TPS | Consumption limiter is the bottleneck |
| 10 TPS | ~10 TPS | Consumption limit and worker capacity are aligned |
| 20 TPS | ~10 TPS | Worker/destination capacity is the bottleneck |
| 100 TPS | ~10 TPS | Higher consumption limit does not increase throughput beyond worker capacity |

The tests reinforce that configuring a high TPS value does not by itself increase throughput. The actual rate is constrained by the slowest relevant stage.

The latest 100 TPS / 1000-message test is being used as the longer observation run for this behavior.

## 18. Important Design Notes

### Worker concurrency is application-side concurrency

`consumer.concurrency` represents the number of application workers processing messages for a product.

### Transport window is different from application concurrency

The JCSMP transport/message window is a messaging transport mechanism. It should not be treated as equivalent to the number of application workers.

### Throughput is not determined by TPS configuration alone

A simplified service-capacity relationship is:

```text
Service Capacity ≈ Concurrency / Processing Latency
```

Example:

```text
Concurrency = 5
Processing latency = 500 ms

Approximate capacity = 5 / 0.5
                     = 10 TPS
```

Therefore a future rate limiter must be considered together with downstream processing capacity.

## 19. Development Principle

The implementation is being developed incrementally.

**The current architecture is frozen at this stage.**

Future features should be added one concept at a time, with the current controlled processing model kept stable unless there is a clear architectural reason to change it.

The current focus is rate limiting and throughput behavior. **Event Logging is intentionally parked for later** and should not influence the current processing-path design until its role and semantics are explicitly decided.

The next design change should be treated independently from the current baseline.
