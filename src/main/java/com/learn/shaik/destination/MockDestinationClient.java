package com.learn.shaik.destination;

import com.learn.shaik.config.ProductConfig;
import com.learn.shaik.model.ProductMessage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MockDestinationClient implements DestinationClient {

    private final long processingTimeMs;

    public MockDestinationClient(ProductConfig.DestinationConfig config) {
        this.processingTimeMs =
                config.getProcessingTimeMs();
    }

    @Override
    public void send(
            String productName,
            ProductMessage message) {

        long started = System.nanoTime();

        log.info(
                "Processing request={} product={} thread={}",
                message.getRequestId(),
                productName,
                Thread.currentThread().getName()
        );

        try {
            Thread.sleep(processingTimeMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Destination processing interrupted", e);
        }

        long latencyMs =
                (System.nanoTime() - started) / 1_000_000;

        log.info(
                "Completed request={} product={} thread={} latencyMs={}",
                message.getRequestId(),
                productName,
                Thread.currentThread().getName(),
                latencyMs
        );
    }
}
