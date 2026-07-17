package com.learnactivemq.publisher.dto;

import java.time.Instant;

public record PublishResponse(String messageId, String eventType, String topic, Instant publishedAt) {
}
