package com.learnactivemq.common.event;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderCreatedEvent(
        String orderId,
        String product,
        int quantity,
        BigDecimal amount,
        Instant createdAt) {
}
