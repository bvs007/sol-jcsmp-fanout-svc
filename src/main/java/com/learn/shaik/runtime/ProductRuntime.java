package com.learn.shaik.runtime;

import com.google.common.util.concurrent.RateLimiter;
import com.learn.shaik.config.ProductConfig;
import com.learn.shaik.destination.DestinationClient;
import com.learn.shaik.model.ProductMessage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProductRuntime {

    private final String productName;
    private final ProductConfig config;
    private final RateLimiter rateLimiter;
    private final DestinationClient destinationClient;

    public ProductRuntime(
            String productName,
            ProductConfig config,
            DestinationClient destinationClient) {

        this.productName = productName;
        this.config = config;
        this.destinationClient = destinationClient;

        this.rateLimiter =
                config.getRateLimit().isEnabled()
                        ? RateLimiter.create(
                                config.getRateLimit().getTps())
                        : null;
    }

    public void process(ProductMessage message) {

        if (rateLimiter != null) {
            rateLimiter.acquire();
        }

        destinationClient.send(
                productName,
                message
        );
    }

    public String getProductName() {
        return productName;
    }

    public ProductConfig getConfig() {
        return config;
    }
}
