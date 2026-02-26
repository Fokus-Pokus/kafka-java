package com.example.kafkarelay.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaRelayListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaRelayListener.class);

    private final JsonTransformer jsonTransformer;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.output-topic}")
    private String outputTopic;

    public KafkaRelayListener(JsonTransformer jsonTransformer,
                              ObjectMapper objectMapper,
                              KafkaTemplate<String, String> kafkaTemplate) {
        this.jsonTransformer = jsonTransformer;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${app.kafka.input-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeAndForward(String message) {
        try {
            String key = jsonTransformer.extractKey(message);
            String outputPayload = objectMapper.writeValueAsString(jsonTransformer.transform(message));
            kafkaTemplate.send(outputTopic, key, outputPayload);
            log.info("Message forwarded to topic='{}' with key='{}'", outputTopic, key);
        } catch (Exception e) {
            log.error("Failed to process message: {}", message, e);
        }
    }
}
