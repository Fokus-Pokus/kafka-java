package com.example.kafkarelay.service;

import com.example.kafkarelay.config.MappingProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class JsonTransformer {

    private final ObjectMapper objectMapper;
    private final MappingProperties mappingProperties;

    public JsonTransformer(ObjectMapper objectMapper, MappingProperties mappingProperties) {
        this.objectMapper = objectMapper;
        this.mappingProperties = mappingProperties;
    }

    public ObjectNode transform(String inputJson) throws Exception {
        JsonNode input = objectMapper.readTree(inputJson);
        ObjectNode result = objectMapper.createObjectNode();

        for (Map.Entry<String, String> mapping : mappingProperties.getFields().entrySet()) {
            JsonNode value = resolvePath(input, mapping.getValue());
            if (value != null && !value.isMissingNode() && !value.isNull()) {
                result.set(mapping.getKey(), value);
            }
        }

        result.put("processedAt", Instant.now().toString());
        return result;
    }

    public String extractKey(String inputJson) throws Exception {
        String keyPath = mappingProperties.getKeyPath();
        if (keyPath == null || keyPath.isBlank()) {
            return null;
        }

        JsonNode input = objectMapper.readTree(inputJson);
        JsonNode keyNode = resolvePath(input, keyPath);
        if (keyNode == null || keyNode.isMissingNode() || keyNode.isNull()) {
            return null;
        }

        return keyNode.isValueNode() ? keyNode.asText() : keyNode.toString();
    }

    private JsonNode resolvePath(JsonNode root, String path) {
        if (path == null || path.isBlank()) {
            return null;
        }

        if (path.startsWith("/")) {
            return root.at(path);
        }

        JsonNode current = root;
        for (String token : path.split("\\.")) {
            if (current == null || current.isMissingNode() || current.isNull()) {
                return null;
            }
            current = current.get(token);
        }
        return current;
    }
}
