package com.learn.shaik.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.shaik.model.ProductMessage;

public final class ProductMessageConverter {

    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper();

    private ProductMessageConverter() {
        /*
         * Utility class.
         */
    }

    public static ProductMessage convert(
            byte[] payload)
            throws Exception {

        return OBJECT_MAPPER.readValue(
                payload,
                ProductMessage.class);
    }
}