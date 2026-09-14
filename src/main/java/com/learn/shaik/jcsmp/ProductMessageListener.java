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

            ProductRuntime runtime =
                    runtimeRegistry.get(productName);

            ProductMessage productMessage =
                    convert(message);

            runtime.process(productMessage);

            /*
             * ACK only after successful downstream processing.
             */
            message.ackMessage();

            log.debug(
                    "Message acknowledged. product={} messageId={}",
                    productName,
                    message.getMessageId()
            );

        } catch (Exception e) {

            log.error(
                    "Message processing failed. product={} messageId={}",
                    productName,
                    message.getMessageId(),
                    e
            );

            /*
             * Deliberately do not ACK.
             *
             * Explicit failure settlement / DMQ policy
             * will be added as the next reliability step.
             */
        }
    }

    private ProductMessage convert(
            BytesXMLMessage message)
            throws Exception {

        byte[] payload =
                message.getAttachmentByteBuffer() == null
                        ? new byte[0]
                        : message.getAttachmentByteBuffer()
                                .array();

        String text =
                new String(
                        payload,
                        StandardCharsets.UTF_8
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
