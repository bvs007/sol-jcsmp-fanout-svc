package com.learn.shaik.jcsmp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.shaik.model.ProductMessage;
import com.learn.shaik.runtime.ProductRuntime;
import com.learn.shaik.runtime.ProductRuntimeRegistry;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.XMLMessageListener;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

@Slf4j
public class ProductMessageListener
        implements XMLMessageListener {

    private final String productName;
    private final ProductRuntimeRegistry runtimeRegistry;
    private final ObjectMapper objectMapper;

    public ProductMessageListener(
            String productName,
            ProductRuntimeRegistry runtimeRegistry) {

        this.productName = productName;
        this.runtimeRegistry = runtimeRegistry;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void onReceive(
            BytesXMLMessage message) {

        try {

            log.debug(
                    "Message received. product={} messageId={} thread={}",
                    productName,
                    message.getMessageId(),
                    Thread.currentThread().getName()
            );

            ProductRuntime runtime =
                    runtimeRegistry.get(productName);

            ProductMessage productMessage =
                    convert(message);

            log.debug(
                    "Message converted. product={} requestId={}",
                    productName,
                    productMessage.getRequestId()
            );

            /*
             * Submit the Solace message + domain message
             * to the product's worker pool.
             *
             * ACK will happen inside the worker after
             * successful destination processing.
             */
            runtime.process(
                    message,
                    productMessage
            );

        } catch (Exception e) {

            log.error(
                    "Message submission failed. product={} messageId={}",
                    productName,
                    message.getMessageId(),
                    e
            );

            /*
             * Do NOT ACK.
             *
             * We will design the full-pool behavior next.
             */
        }
    }

    private ProductMessage convert(
            BytesXMLMessage message)
            throws Exception {

        byte[] payload =
                message.getBytes();

        if (payload == null || payload.length == 0) {

            throw new IllegalArgumentException(
                    "Received message has an empty payload. " +
                            "product=" + productName +
                            ", messageId=" + message.getMessageId()
            );
        }

        String text =
                new String(
                        payload,
                        StandardCharsets.UTF_8
                );

        log.debug(
                "Message payload. product={} messageId={} payload={}",
                productName,
                message.getMessageId(),
                text
        );

        return objectMapper.readValue(
                text,
                ProductMessage.class
        );
    }

    @Override
    public void onException(
            JCSMPException exception) {

        log.error(
                "JCSMP listener exception. product={}",
                productName,
                exception
        );
    }
}