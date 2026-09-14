package com.learn.shaik.test.jcsmp.publisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.shaik.config.ProductConfig;
import com.learn.shaik.config.ProductEngineProperties;
import com.learn.shaik.jcsmp.JcsmpSessionManager;
import com.learn.shaik.model.ProductMessage;
import com.learn.shaik.test.model.TestPublishRequest;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageProducer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Component
public class JcsmpTestPublisher {

    private static final int MAX_TEST_MESSAGES = 1000;

    private final JcsmpSessionManager sessionManager;
    private final ProductEngineProperties productProperties;
    private final ObjectMapper objectMapper;

    private XMLMessageProducer producer;

    public JcsmpTestPublisher(
            JcsmpSessionManager sessionManager,
            ProductEngineProperties productProperties,
            ObjectMapper objectMapper) {

        this.sessionManager = sessionManager;
        this.productProperties = productProperties;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void initialize() throws JCSMPException {

        producer = sessionManager
                .getSession()
                .getMessageProducer(
                        new JCSMPStreamingPublishEventHandler() {

                            @Override
                            public void responseReceived(
                                    String messageId) {

                                log.debug(
                                        "Test message published successfully. messageId={}",
                                        messageId
                                );
                            }

                            @Override
                            public void handleError(
                                    String messageId,
                                    JCSMPException cause,
                                    long timestamp) {

                                log.error(
                                        "Test message publish failed. messageId={} timestamp={}",
                                        messageId,
                                        timestamp,
                                        cause
                                );
                            }
                        }
                );

        log.info("JCSMP test publisher initialized.");
    }

    public List<String> publish(
            String productName,
            TestPublishRequest request) throws Exception {

        if (request == null) {
            request = new TestPublishRequest();
        }

        ProductConfig config =
                productProperties
                        .getProducts()
                        .get(productName);

        if (config == null) {
            throw new IllegalArgumentException(
                    "Unknown product: " + productName
            );
        }

        int count = request.getCount();

        if (count < 1 || count > MAX_TEST_MESSAGES) {
            throw new IllegalArgumentException(
                    "count must be between 1 and "
                            + MAX_TEST_MESSAGES
            );
        }

        Queue queue =
                JCSMPFactory
                        .onlyInstance()
                        .createQueue(config.getQueue());

        List<String> requestIds =
                new ArrayList<>(count);

        for (int i = 1; i <= count; i++) {

            String requestId =
                    productName
                            + "-TEST-"
                            + UUID.randomUUID();

            ProductMessage productMessage =
                    new ProductMessage();

            productMessage.setRequestId(requestId);

            productMessage.setRequestType(
                    request.getRequestType()
            );

            productMessage.setPayload(
                    request.getPayload()
            );

            String json =
                    objectMapper.writeValueAsString(
                            productMessage
                    );

            /*
             * Create a BytesXMLMessage.
             *
             * JCSMP 10.28.1:
             * BytesXMLMessage uses writeBytes(byte[])
             * to write the message payload.
             */
            BytesXMLMessage message =
                    JCSMPFactory
                            .onlyInstance()
                            .createBytesXMLMessage();

            /*
             * Persistent message because we are
             * publishing to a durable queue.
             */
            message.setDeliveryMode(
                    DeliveryMode.PERSISTENT
            );

            message.writeBytes(
                    json.getBytes(
                            StandardCharsets.UTF_8
                    )
            );

            /*
             * Publish to the product's durable queue.
             */
            producer.send(
                    message,
                    queue
            );

            requestIds.add(requestId);

            log.info(
                    "Test message published. " +
                            "product={} queue={} requestId={} sequence={}/{}",
                    productName,
                    config.getQueue(),
                    requestId,
                    i,
                    count
            );
        }

        log.info(
                "Test publishing completed. " +
                        "product={} queue={} count={}",
                productName,
                config.getQueue(),
                count
        );

        return requestIds;
    }

    @PreDestroy
    public void shutdown() {

        if (producer != null) {
            try {

                producer.close();

                log.info(
                        "JCSMP test publisher closed."
                );

            } catch (Exception e) {

                log.warn(
                        "Error while closing JCSMP test publisher.",
                        e
                );
            }
        }
    }
}