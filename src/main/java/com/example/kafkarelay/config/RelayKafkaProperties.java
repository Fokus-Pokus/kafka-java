package com.example.kafkarelay.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka")
public class RelayKafkaProperties {

    private String inputTopic;
    private String outputTopic;

    /**
     * Throttle consumer processing speed. 0 or less means unlimited.
     */
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
