package com.learnactivemq.publisher.dto;

import jakarta.validation.constraints.NotBlank;

public record ShipmentRequest(
        @NotBlank String orderId,
        @NotBlank String carrier,
        @NotBlank String trackingNumber) {
}
