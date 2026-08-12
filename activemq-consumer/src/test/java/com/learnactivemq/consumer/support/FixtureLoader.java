package com.learnactivemq.consumer.support;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.core.io.ClassPathResource;
import tools.jackson.databind.ObjectMapper;

/** Loads a fixtures/*.json file and converts entries into typed event records. */
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

    /** Returns every payload whose type-id matches, converted to {@code targetType}. */
    public <T> List<T> ofType(String type, Class<T> targetType) {
        return fixtures.stream()
                .filter(f -> f.type().equals(type))
                .map(f -> objectMapper.convertValue(f.payload(), targetType))
                .collect(Collectors.toList());
    }
}
