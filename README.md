# multi-threading-engg-jcsmp

JCSMP-based implementation of the controlled Solace consumer engine.

## Current architecture

Solace Durable Queue
    -> JCSMP FlowReceiver
    -> ProductMessageListener
    -> ProductRuntime
    -> Guava RateLimiter
    -> DestinationClient

There is intentionally no large application-level message buffer.

## Configuration-driven onboarding

Products are configured under `product-engine.products`.

Adding a product should require configuration rather than a product-specific
consumer class.

## Initial test values

PRODUCT_A:
- concurrency: 5
- rate limit: 50 TPS
- mock latency: 500 ms
- max unacked target: 5

This gives a theoretical downstream capacity of approximately:

5 concurrent operations / 0.5 seconds = 10 TPS

So the 50 TPS rate limit is not the bottleneck in this initial experiment.

## Solace connection

Set:

SOLACE_HOST
SOLACE_VPN
SOLACE_USERNAME
SOLACE_PASSWORD

before starting the application.

## Important implementation status

This is the first JCSMP implementation, not yet the final production build.

Still to be finalized and tested:

1. Exact broker-side max-delivered-unacked-msgs-per-flow configuration.
2. Exact mapping of flow concurrency to the desired number of active processing
   operations.
3. Explicit FAILED/REJECTED settlement and DMQ/redelivery policy.
4. Payload extraction for the exact producer message type.
5. Reconnect/error handling policy.
6. Metrics for queue depth, in-flight count, actual TPS and latency.
7. Graceful drain behavior during shutdown.
8. Runtime add/remove/update of product flows.

The goal is to establish the control architecture first and then harden each
Solace-specific reliability boundary with integration tests.
