package com.example.kafkarelay.service;

import com.example.kafkarelay.config.RelayKafkaProperties;
import com.example.kafkarelay.config.RelayKafkaProperties.Route;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaRelayListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaRelayListener.class);

    private final JsonTransformer jsonTransformer;
    private final ObjectMapper objectMapper;
    private final RelayKafkaProperties relayKafkaProperties;
    private final ConsumptionRateLimiter consumptionRateLimiter;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaRelayListener(JsonTransformer jsonTransformer,
                              ObjectMapper objectMapper,
                              RelayKafkaProperties relayKafkaProperties,
                              ConsumptionRateLimiter consumptionRateLimiter,
                              KafkaTemplate<String, String> kafkaTemplate) {
        this.jsonTransformer = jsonTransformer;
        this.objectMapper = objectMapper;
        this.relayKafkaProperties = relayKafkaProperties;
        this.consumptionRateLimiter = consumptionRateLimiter;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
            id = "route1-listener",
            topics = "${app.kafka.routes.route1.input-topic}",
            groupId = "${app.kafka.user.group-id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consumeRoute1(String message) {
        relay("route1", message);
    }

    @KafkaListener(
            id = "route3-listener",
            topics = "${app.kafka.routes.route3.input-topic}",
            groupId = "${app.kafka.user.group-id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consumeRoute3(String message) {
        relay("route3", message);
    }

    private void relay(String routeName, String message) {
        try {
            Route route = route(routeName);
            consumptionRateLimiter.throttle(routeName, route.getMaxMessagesPerSecond());

            String key = jsonTransformer.extractKey(message);
            String outputPayload = objectMapper.writeValueAsString(jsonTransformer.transform(message));
            kafkaTemplate.send(route.getOutputTopic(), key, outputPayload);
            log.info("Message forwarded: route='{}', topic='{}', key='{}'",
                    routeName, route.getOutputTopic(), key);
        } catch (Exception e) {
            log.error("Failed to process message on route='{}': {}", routeName, message, e);
        }
    }

    private Route route(String routeName) {
        Route route = relayKafkaProperties.getRoutes().get(routeName);
        if (route == null) {
            throw new IllegalArgumentException("Route not configured: " + routeName);
        }
        return route;
    }
}
