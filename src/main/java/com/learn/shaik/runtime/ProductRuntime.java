package com.learn.shaik.runtime;

import com.learn.shaik.config.ProductConfig;
import com.learn.shaik.destination.DestinationClient;
import com.learn.shaik.model.ProductMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.FlowReceiver;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.learn.shaik.ratelimit.impl.GuavaMessageRateLimiter;
import com.learn.shaik.ratelimit.MessageRateLimiter;
import com.learn.shaik.ratelimit.impl.NoOpMessageRateLimiter;

@Slf4j
public class ProductRuntime {

    private final String productName;

    private final ProductConfig config;

    private final DestinationClient destinationClient;
    private final ExecutorService workers;

    private final MessageRateLimiter consumptionRateLimiter;

    private volatile boolean running;

    /*
     * This executor does NOT act as a message queue.
     *
     * It owns exactly N long-running worker threads.
     */

    public ProductRuntime(
            String productName,
            ProductConfig config,
            DestinationClient destinationClient) {

        this.productName = productName;
        this.config = config;
        this.destinationClient = destinationClient;

        int concurrency =
                config.getConsumer().getConcurrency();

        this.workers =
                Executors.newFixedThreadPool(concurrency);

        this.consumptionRateLimiter =
        createConsumptionRateLimiter(config);

        log.info(
                "Product runtime initialized. product={} workers={}",
                productName,
                concurrency);
    }

    /**
     * Starts N workers.
     *
     * One FlowReceiver is shared by all workers.
     */
    public void startWorkers(FlowReceiver flow) {

        if (running) {
            log.warn(
                    "Workers already running. product={}",
                    productName);
            return;
        }

        running = true;

        int concurrency =
                config.getConsumer().getConcurrency();

        for (int i = 0; i < concurrency; i++) {

            int workerNumber = i + 1;

            workers.submit(
                    () -> workerLoop(
                            flow,
                            workerNumber));
        }

        log.info(
                "Product workers started. product={} workers={}",
                productName,
                concurrency);
    }

    /**
     * Long-running worker.
     *
     * Each worker:
     *
     * 1. Receives one message
     * 2. Processes it
     * 3. ACKs it after successful processing
     * 4. Goes back to receive another message
     */
    private void workerLoop(
            FlowReceiver flow,
            int workerNumber) {

        String workerName =
                "product-"
                        + productName
                        + "-worker-"
                        + workerNumber;

        log.info(
                "Worker started. product={} worker={}",
                productName,
                workerName);

        while (running) {

            BytesXMLMessage message = null;

            try {

                 consumptionRateLimiter.acquire();

                /*
                 * Application-controlled synchronous receive.
                 *
                 * 1000ms timeout allows the worker to periodically
                 * check the 'running' flag during shutdown.
                 */
                message =
                        flow.receive(1000);

                if (message == null) {
                    continue;
                }

                log.debug(
                        "Message received. product={} worker={} messageId={}",
                        productName,
                        workerName,
                        message.getMessageId());

                /*
                 * Process the message.
                 */
                process(
                        message,
                        workerName);

                /*
                 * ACK ONLY after successful processing.
                 */
                message.ackMessage();

                log.info(
                        "Message acknowledged. product={} worker={} messageId={}",
                        productName,
                        workerName,
                        message.getMessageId());

            } catch (Exception e) {

                log.error(
                        "Message processing failed. product={} worker={} messageId={}",
                        productName,
                        workerName,
                        message != null
                                ? message.getMessageId()
                                : null,
                        e);

                /*
                 * DO NOT ACK here.
                 *
                 * Retry / redelivery / DMQ behaviour
                 * will be designed separately.
                 */
            }
        }

        log.info(
                "Worker stopped. product={} worker={}",
                productName,
                workerName);
    }

    /**
     * Processes one Solace message.
     */
    private void process(
            BytesXMLMessage message,
            String workerName)
            throws Exception {

        byte[] payload =
                message.getBytes();

        ProductMessage productMessage =
                ProductMessageConverter.convert(
                        payload);

        log.info(
                "Processing request. product={} worker={} requestId={}",
                productName,
                workerName,
                productMessage.getRequestId());

        destinationClient.send(
                productName,
                productMessage);
    }

    /**
     * Gracefully stops workers.
     */
    public void shutdown() {

        log.info(
                "Stopping product runtime. product={}",
                productName);

        running = false;

        workers.shutdown();

        try {

            if (!workers.awaitTermination(
                    10,
                    TimeUnit.SECONDS)) {

                log.warn(
                        "Workers did not terminate within timeout. "
                                + "Forcing shutdown. product={}",
                        productName);

                workers.shutdownNow();
            }

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            log.warn(
                    "Interrupted while stopping workers. "
                            + "Forcing shutdown. product={}",
                    productName);

            workers.shutdownNow();
        }

        log.info(
                "Product runtime stopped. product={}",
                productName);
    }

    private MessageRateLimiter createConsumptionRateLimiter(
            ProductConfig config) {

        ProductConfig.RateLimitConfig rateLimit =
                config.getConsumer().getRateLimit();

        if (rateLimit == null ||
                !rateLimit.isEnabled()) {
            log.info("Rate Limit disabled for Product Name: {}", productName);
            return new NoOpMessageRateLimiter();
        }

        log.info("Rate Limit: {}TPS for Product Name: {} of {} workers",  rateLimit.getTps(), productName, config.getConsumer().getConcurrency());
        return new GuavaMessageRateLimiter(
                rateLimit.getTps());
    }
}