package com.example.kafkarelay.service;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ConsumptionRateLimiterTest {

    @Test
    void shouldThrottleWhenRateIsLimited() {
        ConsumptionRateLimiter limiter = new ConsumptionRateLimiter();

        long started = System.nanoTime();
        limiter.throttle(5.0);
        limiter.throttle(5.0);
        long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

        assertTrue(elapsedMillis >= 150, "Expected throttling delay to be applied");
    }

    @Test
    void shouldNotThrottleWhenUnlimited() {
        ConsumptionRateLimiter limiter = new ConsumptionRateLimiter();

        long started = System.nanoTime();
        limiter.throttle(0);
        limiter.throttle(-1);
        long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

        assertTrue(elapsedMillis < 100, "Unlimited mode should not add notable delay");
    }
}
