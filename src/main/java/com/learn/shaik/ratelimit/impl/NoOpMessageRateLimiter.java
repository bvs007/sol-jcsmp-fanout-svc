package com.learn.shaik.ratelimit.impl;

import com.learn.shaik.ratelimit.MessageRateLimiter;

public class NoOpMessageRateLimiter
        implements MessageRateLimiter {

    @Override
    public void acquire() {
        // Intentionally does nothing.
    }
}