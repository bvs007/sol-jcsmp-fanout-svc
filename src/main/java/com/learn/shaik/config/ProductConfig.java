package com.learn.shaik.config;

import lombok.Data;

@Data
public class ProductConfig {

    private String queue;
    private ConsumerConfig consumer;
    private DestinationConfig destination;

    @Data
    public static class ConsumerConfig {
        private int concurrency;
        private RateLimitConfig rateLimit;
    }

    @Data
    public static class RateLimitConfig {
        private boolean enabled;
        private int tps;
    }

    @Data
    public static class DestinationConfig {
        private String type;
        private long processingTimeMs;
    }
}
