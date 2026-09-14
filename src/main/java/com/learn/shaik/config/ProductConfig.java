package com.learn.shaik.config;

import lombok.Data;

@Data
public class ProductConfig {

    private String queue;
    private ConsumerConfig consumer;
    private RateLimitConfig rateLimit;
    private DestinationConfig destination;

    @Data
    public static class ConsumerConfig {
        private int concurrency;
        private int maxUnackedMessages;
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
