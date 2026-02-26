package com.example.kafkarelay.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaClientsConfig {

    private final RelayKafkaProperties properties;

    public KafkaClientsConfig(RelayKafkaProperties properties) {
        this.properties = properties;
    }

    @Bean
    public ConsumerFactory<String, String> kafkaConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProps());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory());
        return factory;
    }

    @Bean
    public ProducerFactory<String, String> kafkaProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerProps());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(kafkaProducerFactory());
    }

    private Map<String, Object> consumerProps() {
        RelayKafkaProperties.User user = properties.getUser();
        Map<String, Object> config = commonClientProps();
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, user.getGroupId());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return config;
    }

    private Map<String, Object> producerProps() {
        Map<String, Object> config = commonClientProps();
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return config;
    }

    private Map<String, Object> commonClientProps() {
        RelayKafkaProperties.User user = properties.getUser();
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, properties.getSecurity().getProtocol());
        config.put(SaslConfigs.SASL_MECHANISM, properties.getSecurity().getSaslMechanism());
        config.put(SaslConfigs.SASL_JAAS_CONFIG, jaas(user));
        config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, properties.getSecurity().getTruststoreLocation());
        config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, properties.getSecurity().getTruststorePassword());
        config.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, properties.getSecurity().getTruststoreType());
        return config;
    }

    private String jaas(RelayKafkaProperties.User user) {
        return "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                + user.getUsername() + "\" password=\"" + user.getPassword() + "\";";
    }
}
