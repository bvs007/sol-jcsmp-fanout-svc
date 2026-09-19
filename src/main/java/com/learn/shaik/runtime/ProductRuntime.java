package com.learn.shaik.runtime;

import com.google.common.util.concurrent.RateLimiter;
import com.learn.shaik.config.ProductConfig;
import com.learn.shaik.destination.DestinationClient;
import com.learn.shaik.model.ProductMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ProductRuntime {

    private final String productName;
    private final ProductConfig config;
    private final RateLimiter rateLimiter;
    private final DestinationClient destinationClient;

    private final ThreadPoolExecutor workerPool;

    public ProductRuntime(
            String productName,
            ProductConfig config,
            DestinationClient destinationClient) {

        this.productName = productName;
        this.config = config;
        this.destinationClient = destinationClient;

        /*
         * Rate limiter is retained as-is for now.
         *
         * We will revisit where the permit acquisition
         * should happen after the worker-pool behavior
         * is verified.
         */
        this.rateLimiter =
                config.getRateLimit().isEnabled()
                        ? RateLimiter.create(
                        config.getRateLimit().getTps())
                        : null;

        int concurrency =
                config.getConsumer().getConcurrency();

        /*
         * No application-side waiting queue.
         *
         * SynchronousQueue has ZERO capacity.
         *
         * Therefore a submitted task must be handed
         * directly to an available worker.
         */
        this.workerPool =
                new ThreadPoolExecutor(
                        concurrency,
                        concurrency,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new SynchronousQueue<>(),
                        new ThreadPoolExecutor.AbortPolicy()
                );

        log.info(
                "Product runtime initialized. product={} workers={} applicationQueueCapacity=0",
                productName,
                concurrency
        );
    }

    /**
     * Submit a message for processing.
     *
     * The Solace message is retained by the processing
     * task so that ACK can happen only after successful
     * downstream processing.
     */
    public void process(
            BytesXMLMessage solaceMessage,
            ProductMessage productMessage) {

        workerPool.execute(() -> {

            try {

                log.debug(
                        "Worker started. product={} messageId={} requestId={} thread={}",
                        productName,
                        solaceMessage.getMessageId(),
                        productMessage.getRequestId(),
                        Thread.currentThread().getName()
                );

                /*
                 * Rate limiting behavior remains unchanged
                 * for this step.
                 */
                if (rateLimiter != null) {
                    rateLimiter.acquire();
                }

                destinationClient.send(
                        productName,
                        productMessage
                );

                /*
                 * ACK ONLY after successful destination processing.
                 */
                solaceMessage.ackMessage();

                log.info(
                        "Message acknowledged. product={} messageId={} requestId={} thread={}",
                        productName,
                        solaceMessage.getMessageId(),
                        productMessage.getRequestId(),
                        Thread.currentThread().getName()
                );

            } catch (Exception e) {

                log.error(
                        "Worker processing failed. product={} messageId={} requestId={}",
                        productName,
                        solaceMessage.getMessageId(),
                        productMessage.getRequestId(),
                        e
                );

                /*
                 * DO NOT ACK.
                 *
                 * Retry / redelivery / DMQ behavior will
                 * be designed separately.
                 */
            }
        });
    }

    public String getProductName() {
        return productName;
    }

    public ProductConfig getConfig() {
        return config;
    }

    public int getActiveWorkerCount() {
        return workerPool.getActiveCount();
    }

    public int getPoolSize() {
        return workerPool.getPoolSize();
    }

    public void shutdown() {

        log.info(
                "Shutting down worker pool. product={}",
                productName
        );

        workerPool.shutdown();

        try {

            if (!workerPool.awaitTermination(
                    10,
                    TimeUnit.SECONDS)) {

                log.warn(
                        "Worker pool did not terminate gracefully. product={}",
                        productName
                );

                workerPool.shutdownNow();
            }

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            workerPool.shutdownNow();
        }
    }
}