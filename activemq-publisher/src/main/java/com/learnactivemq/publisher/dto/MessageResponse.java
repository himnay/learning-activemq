package com.learnactivemq.publisher.dto;

import java.time.Instant;

public record MessageResponse(String messageId, String queue, Instant publishedAt) {
}
