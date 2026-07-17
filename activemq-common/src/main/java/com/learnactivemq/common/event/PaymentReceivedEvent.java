package com.learnactivemq.common.event;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentReceivedEvent(
        String paymentId,
        String orderId,
        BigDecimal amount,
        String method,
        Instant receivedAt) {
}
