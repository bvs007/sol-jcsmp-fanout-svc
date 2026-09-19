package com.learn.shaik.jcsmp;

import com.learn.shaik.config.ProductConfig;
import com.learn.shaik.config.ProductEngineProperties;
import com.learn.shaik.runtime.ProductRuntime;
import com.learn.shaik.runtime.ProductRuntimeRegistry;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.Queue;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class JcsmpProductConsumer {

    private final JCSMPSession session;

    private final ProductEngineProperties properties;

    private final ProductRuntimeRegistry runtimeRegistry;

    /*
     * One FlowReceiver per product.
     */
    private final Map<String, FlowReceiver> flows =
            new ConcurrentHashMap<>();

public JcsmpProductConsumer(
        JcsmpSessionManager sessionManager,
        ProductEngineProperties properties,
        ProductRuntimeRegistry runtimeRegistry) {

    this.session = sessionManager.getSession();
    this.properties = properties;
    this.runtimeRegistry = runtimeRegistry;
}
    /**
     * Starts all configured products.
     */
    @PostConstruct
    public void start()
            throws JCSMPException {

        for (Map.Entry<String, ProductConfig> entry
                : properties.getProducts().entrySet()) {

            String productName =
                    entry.getKey();

            ProductConfig config =
                    entry.getValue();

            startProduct(
                    productName,
                    config);
        }
    }

    /**
     * Creates one FlowReceiver for one product.
     */
    private void startProduct(
            String productName,
            ProductConfig config)
            throws JCSMPException {

        Queue queue =
                JCSMPFactory
                        .onlyInstance()
                        .createQueue(
                                config.getQueue());

        ConsumerFlowProperties flowProperties =
                new ConsumerFlowProperties();

        flowProperties.setEndpoint(queue);

        /*
         * Client acknowledgement.
         */
        flowProperties.setAckMode(
                JCSMPProperties
                        .SUPPORTED_MESSAGE_ACK_CLIENT);

        /*
         * JCSMP transport window.
         *
         * This is NOT our application concurrency control.
         */
        flowProperties.setTransportWindowSize(
                config.getConsumer()
                        .getConcurrency());

        /*
         * NULL listener is intentional.
         *
         * This creates a synchronous FlowReceiver.
         */
        FlowReceiver flow =
                session.createFlow(
                        null,
                        flowProperties,
                        null);

        flow.start();

        flows.put(
                productName,
                flow);

        ProductRuntime runtime =
                runtimeRegistry.getRequired(
                        productName);

        /*
         * Start N application workers against
         * this ONE FlowReceiver.
         */
        runtime.startWorkers(flow);

        log.info(
                "Synchronous JCSMP flow started. "
                        + "product={} queue={} workers={} transportWindow={}",
                productName,
                config.getQueue(),
                config.getConsumer()
                        .getConcurrency(),
                config.getConsumer()
                        .getConcurrency());
    }

    /**
 * Stops all product workers and flows.
 */
@PreDestroy
public void stop() {

    log.info("Stopping JCSMP product consumers");

    /*
     * 1. Stop application workers first.
     *
     * ProductRuntime.shutdown() sets running=false
     * and waits for the worker threads to finish.
     */
    runtimeRegistry.shutdown();

    /*
     * 2. Now that workers have stopped,
     *    stop and close the FlowReceivers.
     */
    flows.forEach(
            (productName, flow) -> {

                try {

                    flow.stop();
                    flow.close();

                    log.info(
                            "JCSMP flow stopped. product={}",
                            productName);

                } catch (Exception e) {

                    log.warn(
                            "Failed to close JCSMP flow. product={}",
                            productName,
                            e);
                }
            });

    flows.clear();

    log.info("JCSMP product consumers stopped");
}
}