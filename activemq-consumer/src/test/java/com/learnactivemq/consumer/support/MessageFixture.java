package com.learnactivemq.consumer.support;

import tools.jackson.databind.JsonNode;

/**
 * One entry of a fixtures/*.json file: a message type-id (matching
 * JmsEventConverterConfig's typeIdMappings) paired with its raw payload,
 * deserialized on demand into the concrete event record the test needs.
 */
public record MessageFixture(String type, JsonNode payload) {
}
