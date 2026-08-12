package com.learnactivemq.publisher.support;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.springframework.core.io.ClassPathResource;
import tools.jackson.databind.ObjectMapper;

/** Loads a fixtures/*.json file and exposes entries filtered by type-id. */
public class FixtureLoader {

    private final ObjectMapper objectMapper;
    private final List<MessageFixture> fixtures;

    public FixtureLoader(ObjectMapper objectMapper, String classpathLocation) {
        this.objectMapper = objectMapper;
        try (InputStream in = new ClassPathResource(classpathLocation).getInputStream()) {
            this.fixtures = objectMapper.readValue(in, objectMapper.getTypeFactory()
                    .constructCollectionType(List.class, MessageFixture.class));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load fixture file: " + classpathLocation, e);
        }
    }

    public List<MessageFixture> ofType(String type) {
        return fixtures.stream().filter(f -> f.type().equals(type)).toList();
    }

    public <T> T payloadAs(MessageFixture fixture, Class<T> targetType) {
        return objectMapper.convertValue(fixture.payload(), targetType);
    }
}
