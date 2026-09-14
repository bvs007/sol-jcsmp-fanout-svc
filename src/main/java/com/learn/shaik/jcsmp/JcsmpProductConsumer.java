package com.learn.shaik.jcsmp;

import com.learn.shaik.config.ProductConfig;
import com.learn.shaik.config.ProductEngineProperties;
import com.learn.shaik.runtime.ProductRuntimeRegistry;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageListener;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class JcsmpProductConsumer {

    private final JcsmpSessionManager sessionManager;
    private final ProductEngineProperties properties;
    private final ProductRuntimeRegistry runtimeRegistry;

    private final Map<String, FlowReceiver> flows =
            new ConcurrentHashMap<>();

    @PostConstruct
    public void start() {

        properties.getProducts()
                .forEach(this::createFlow);
    }

    private void createFlow(
            String productName,
            ProductConfig config) {

        try {
            Queue queue =
                    JCSMPFactory.onlyInstance()
                            .createQueue(config.getQueue());

            ConsumerFlowProperties flowProperties =
                    new ConsumerFlowProperties();

            flowProperties.setEndpoint(queue);

            flowProperties.setAckMode(
                    JCSMPProperties
                            .SUPPORTED_MESSAGE_ACK_CLIENT
            );

            flowProperties.setStartState(true);

            XMLMessageListener listener =
                    new ProductMessageListener(
                            productName,
                            runtimeRegistry
                    );

            FlowReceiver flow =
                    sessionManager
                            .getSession()
                            .createFlow(
                                    listener,
                                    flowProperties
                            );

            flow.start();

            flows.put(
                    productName,
                    flow
            );

            log.info(
                    "JCSMP flow started. product={} queue={} concurrency={} maxUnacked={}",
                    productName,
                    config.getQueue(),
                    config.getConsumer().getConcurrency(),
                    config.getConsumer().getMaxUnackedMessages()
            );

        } catch (JCSMPException e) {

            throw new IllegalStateException(
                    "Failed to create JCSMP flow for "
                            + productName,
                    e
            );
        }
    }

    @PreDestroy
    public void stop() {

        flows.forEach((productName, flow) -> {
            try {
                flow.close();

                log.info(
                        "JCSMP flow stopped. product={}",
                        productName
                );

            } catch (Exception e) {

                log.error(
                        "Failed to close flow. product={}",
                        productName,
                        e
                );
            }
        });

        flows.clear();
    }

    public int getFlowCount() {
        return flows.size();
    }
}
