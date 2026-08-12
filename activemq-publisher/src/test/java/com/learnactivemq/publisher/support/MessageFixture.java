package com.learnactivemq.publisher.support;

import tools.jackson.databind.JsonNode;

/**
 * One entry of a fixtures/*.json file: a message type-id paired with its raw
 * payload (and an optional bulk-publish count), deserialized on demand into
 * the concrete request DTO the test needs.
 */
public record MessageFixture(String type, Integer count, JsonNode payload) {
}
