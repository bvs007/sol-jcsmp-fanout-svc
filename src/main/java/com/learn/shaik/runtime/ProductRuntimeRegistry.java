package com.learn.shaik.runtime;

import com.learn.shaik.config.ProductConfig;
import com.learn.shaik.config.ProductEngineProperties;
import com.learn.shaik.destination.DestinationClient;
import com.learn.shaik.destination.MockDestinationClient;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import jakarta.annotation.PreDestroy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProductRuntimeRegistry {

    private final ProductEngineProperties properties;

    private final Map<String, ProductRuntime> runtimes =
            new ConcurrentHashMap<>();

    @PostConstruct
    public void initialize() {

        if (properties.getProducts() == null ||
                properties.getProducts().isEmpty()) {
            throw new IllegalStateException(
                    "No products configured");
        }

        properties.getProducts()
                .forEach(this::createRuntime);

        log.info(
                "Product runtimes initialized. count={}",
                runtimes.size()
        );
    }

    private void createRuntime(
            String productName,
            ProductConfig config) {

        validate(productName, config);

        DestinationClient destinationClient =
                new MockDestinationClient(config);

        runtimes.put(
                productName,
                new ProductRuntime(
                        productName,
                        config,
                        destinationClient
                )
        );
    }

    private void validate(
            String productName,
            ProductConfig config) {

        if (config.getQueue() == null ||
                config.getQueue().isBlank()) {
            throw new IllegalStateException(
                    "Queue not configured for " + productName);
        }

        if (config.getConsumer() == null ||
                config.getConsumer().getConcurrency() <= 0) {
            throw new IllegalStateException(
                    "Invalid concurrency for " + productName);
        }

        if (config.getConsumer().getMaxUnackedMessages() <= 0) {
            throw new IllegalStateException(
                    "Invalid max-unacked-messages for " + productName);
        }

        if (config.getRateLimit() == null ||
                config.getRateLimit().getTps() <= 0) {
            throw new IllegalStateException(
                    "Invalid rate limit for " + productName);
        }

        if (config.getDestination() == null) {
            throw new IllegalStateException(
                    "Destination not configured for " + productName);
        }
    }

    public ProductRuntime get(String productName) {

        ProductRuntime runtime = runtimes.get(productName);

        if (runtime == null) {
            throw new IllegalArgumentException(
                    "No runtime configured for " + productName);
        }

        return runtime;
    }

    public Map<String, ProductRuntime> getAll() {
        return Map.copyOf(runtimes);
    }

    @PreDestroy
    public void shutdown() {

        log.info("Shutting down all product runtimes");

        runtimes.forEach(
                (productName, runtime) -> {
                    try {
                        runtime.shutdown();
                    } catch (Exception e) {
                        log.error(
                                "Failed to shutdown product runtime. product={}",
                                productName,
                                e
                        );
                    }
                }
        );

        runtimes.clear();
    }
}
