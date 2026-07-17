package com.learnactivemq.publisher.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;

public record OrderRequest(
        @NotBlank String product,
        @Positive int quantity,
        @NotNull @Positive BigDecimal amount) {
}
