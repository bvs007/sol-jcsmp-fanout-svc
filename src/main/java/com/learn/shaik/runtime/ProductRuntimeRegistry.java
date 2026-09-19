package com.learn.shaik.runtime;

import com.learn.shaik.config.ProductConfig;
import com.learn.shaik.config.ProductEngineProperties;
import com.learn.shaik.destination.DestinationClient;
import com.learn.shaik.destination.MockDestinationClient;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ProductRuntimeRegistry {

    private final Map<String, ProductRuntime> runtimes =
            new ConcurrentHashMap<>();

    public ProductRuntimeRegistry(
            ProductEngineProperties properties) {

        properties.getProducts()
                .forEach(
                        (productName, config) -> {

                            DestinationClient destinationClient =
                                    createDestinationClient(
                                            productName,
                                            config);

                            ProductRuntime runtime =
                                    new ProductRuntime(
                                            productName,
                                            config,
                                            destinationClient);

                            runtimes.put(
                                    productName,
                                    runtime);
                        });
    }

    /**
     * Returns runtime for a product.
     */
    public ProductRuntime getRequired(
            String productName) {

        ProductRuntime runtime =
                runtimes.get(productName);

        if (runtime == null) {

            throw new IllegalArgumentException(
                    "No runtime configured for product="
                            + productName);
        }

        return runtime;
    }

    /**
     * Creates destination client.
     *
     * Keep your existing destination selection logic here
     * if you already have HTTP/MOCK implementations.
     */
    private DestinationClient createDestinationClient(
            String productName,
            ProductConfig config) {

        return new MockDestinationClient(
                config.getDestination());
    }

    /**
     * Shutdown all product runtimes.
     */
    public void shutdown() {

        runtimes.values()
                .forEach(
                        ProductRuntime::shutdown);
    }
}