package com.example.kafkarelay.config;

import jakarta.annotation.PostConstruct;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class RelayKafkaPropertiesValidator {

    private static final List<String> REQUIRED_ROUTES = List.of("route1", "route3");

    private final RelayKafkaProperties properties;

    public RelayKafkaPropertiesValidator(RelayKafkaProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public void validate() {
        RelayKafkaProperties.User user = properties.getUser();
        if (user == null || isBlank(user.getUsername()) || isBlank(user.getPassword()) || isBlank(user.getGroupId())) {
            throw new IllegalStateException("app.kafka.user.username/password/group-id must be configured");
        }

        REQUIRED_ROUTES.forEach(routeName -> {
            RelayKafkaProperties.Route route = properties.getRoutes().get(routeName);
            if (route == null) {
                throw new IllegalStateException("Missing kafka route config: app.kafka.routes." + routeName);
            }
            if (isBlank(route.getInputTopic()) || isBlank(route.getOutputTopic())) {
                throw new IllegalStateException("Route must contain input-topic and output-topic: " + routeName);
            }
        });
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
