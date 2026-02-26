package com.example.kafkarelay.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.example.kafkarelay.config.MappingProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonTransformerTest {

    private JsonTransformer transformer;

    @BeforeEach
    void setUp() {
        MappingProperties properties = new MappingProperties();
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("requestId", "messageId");
        fields.put("userId", "payload.user.id");
        properties.setFields(fields);
        properties.setKeyPath("messageId");

        transformer = new JsonTransformer(new ObjectMapper(), properties);
    }

    @Test
    void shouldTransformAndExtractKey() throws Exception {
        String input = """
            {
              "messageId": "abc-123",
              "payload": {
                "user": {
                  "id": "u-42"
                }
              }
            }
            """;

        var output = transformer.transform(input);

        assertEquals("abc-123", output.get("requestId").asText());
        assertEquals("u-42", output.get("userId").asText());
        assertEquals("abc-123", transformer.extractKey(input));
    }

    @Test
    void shouldReturnNullKeyWhenKeyPathMissing() throws Exception {
        String input = "{" +
                "\"payload\": {\"user\": {\"id\": \"u-42\"}}" +
                "}";

        assertNull(transformer.extractKey(input));
    }
}
