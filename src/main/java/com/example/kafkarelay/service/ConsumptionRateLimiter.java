package com.example.kafkarelay.service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.springframework.stereotype.Component;

@Component
public class ConsumptionRateLimiter {

    private final ConcurrentHashMap<String, AtomicLong> nextAllowedNanosByKey = new ConcurrentHashMap<>();

    public void throttle(String key, double messagesPerSecond) {
        if (messagesPerSecond <= 0) {
            return;
        }

        AtomicLong nextAllowedNanos = nextAllowedNanosByKey.computeIfAbsent(key, ignored -> new AtomicLong(0));
        long intervalNanos = (long) (TimeUnit.SECONDS.toNanos(1) / messagesPerSecond);
        long now = System.nanoTime();
        long scheduled = nextAllowedNanos.updateAndGet(previous -> previous <= now ? now + intervalNanos : previous + intervalNanos);
        long waitNanos = scheduled - intervalNanos - now;
        if (waitNanos > 0) {
            LockSupport.parkNanos(waitNanos);
        }
    }
}
