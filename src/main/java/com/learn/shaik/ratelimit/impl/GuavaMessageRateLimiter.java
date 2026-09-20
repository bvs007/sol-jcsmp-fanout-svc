package com.learn.shaik.ratelimit.impl;

import com.google.common.util.concurrent.RateLimiter;
import com.learn.shaik.ratelimit.MessageRateLimiter;

public class GuavaMessageRateLimiter
        implements MessageRateLimiter {

    private final RateLimiter rateLimiter;

    public GuavaMessageRateLimiter(double permitsPerSecond) {

        if (permitsPerSecond <= 0) {
            throw new IllegalArgumentException(
                    "Rate limit must be greater than zero");
        }

        this.rateLimiter =
                RateLimiter.create(permitsPerSecond);
    }

    @Override
    public void acquire() {

        rateLimiter.acquire();
    }
}