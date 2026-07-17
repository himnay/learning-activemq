package com.learnactivemq.common.event;

import java.time.Instant;

public record ShipmentDispatchedEvent(
        String shipmentId,
        String orderId,
        String carrier,
        String trackingNumber,
        Instant dispatchedAt) {
}
