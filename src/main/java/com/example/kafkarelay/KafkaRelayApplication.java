package com.example.kafkarelay;

import com.example.kafkarelay.config.MappingProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(MappingProperties.class)
public class KafkaRelayApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaRelayApplication.class, args);
    }
}
