package com.learnactivemq.publisher.dto;

import java.time.Instant;

public record BulkPublishResponse(int count, String topic, Instant publishedAt) {
}
