package com.example.kafkarelay.config;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka")
public class RelayKafkaProperties {

    private String bootstrapServers;
    private Security security = new Security();
    private User user = new User();
    private Map<String, Route> routes = new LinkedHashMap<>();

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Security getSecurity() {
        return security;
    }

    public void setSecurity(Security security) {
        this.security = security;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public Map<String, Route> getRoutes() {
        return routes;
    }

    public void setRoutes(Map<String, Route> routes) {
        this.routes = routes;
    }

    public static class Security {
        private String protocol = "SASL_SSL";
        private String saslMechanism = "PLAIN";
        private String truststoreLocation;
        private String truststorePassword;
        private String truststoreType = "JKS";

        public String getProtocol() {
            return protocol;
        }

        public void setProtocol(String protocol) {
            this.protocol = protocol;
        }

        public String getSaslMechanism() {
            return saslMechanism;
        }

        public void setSaslMechanism(String saslMechanism) {
            this.saslMechanism = saslMechanism;
        }

        public String getTruststoreLocation() {
            return truststoreLocation;
        }

        public void setTruststoreLocation(String truststoreLocation) {
            this.truststoreLocation = truststoreLocation;
        }

        public String getTruststorePassword() {
            return truststorePassword;
        }

        public void setTruststorePassword(String truststorePassword) {
            this.truststorePassword = truststorePassword;
        }

        public String getTruststoreType() {
            return truststoreType;
        }

        public void setTruststoreType(String truststoreType) {
            this.truststoreType = truststoreType;
        }
    }

    public static class User {
        private String username;
        private String password;
        private String groupId;

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }
    }

    public static class Route {
        private String inputTopic;
        private String outputTopic;
        private double maxMessagesPerSecond = 0;

        public String getInputTopic() {
            return inputTopic;
        }

        public void setInputTopic(String inputTopic) {
            this.inputTopic = inputTopic;
        }

        public String getOutputTopic() {
            return outputTopic;
        }

        public void setOutputTopic(String outputTopic) {
            this.outputTopic = outputTopic;
        }

        public double getMaxMessagesPerSecond() {
            return maxMessagesPerSecond;
        }

        public void setMaxMessagesPerSecond(double maxMessagesPerSecond) {
            this.maxMessagesPerSecond = maxMessagesPerSecond;
        }
    }
}
