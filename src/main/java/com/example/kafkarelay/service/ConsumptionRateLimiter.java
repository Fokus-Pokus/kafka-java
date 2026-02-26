package com.example.kafkarelay.service;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.springframework.stereotype.Component;

@Component
public class ConsumptionRateLimiter {

    private final AtomicLong nextAllowedNanos = new AtomicLong(0);

    public void throttle(double messagesPerSecond) {
        if (messagesPerSecond <= 0) {
            return;
        }

        long intervalNanos = (long) (TimeUnit.SECONDS.toNanos(1) / messagesPerSecond);
        long now = System.nanoTime();
        long scheduled = nextAllowedNanos.updateAndGet(previous -> previous <= now ? now + intervalNanos : previous + intervalNanos);
        long waitNanos = scheduled - intervalNanos - now;
        if (waitNanos > 0) {
            LockSupport.parkNanos(waitNanos);
        }
    }
}
