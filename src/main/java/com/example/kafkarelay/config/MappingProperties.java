package com.example.kafkarelay.config;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.mapping")
public class MappingProperties {

    /**
     * outputField -> input path mapping.
     * Path supports dot notation (payload.user.id) and JSON Pointer (/payload/user/id).
     */
    private Map<String, String> fields = new LinkedHashMap<>();

    /**
     * Optional path for message key extraction.
     */
    private String keyPath;

    public Map<String, String> getFields() {
        return fields;
    }

    public void setFields(Map<String, String> fields) {
        this.fields = fields;
    }

    public String getKeyPath() {
        return keyPath;
    }

    public void setKeyPath(String keyPath) {
        this.keyPath = keyPath;
    }
}
