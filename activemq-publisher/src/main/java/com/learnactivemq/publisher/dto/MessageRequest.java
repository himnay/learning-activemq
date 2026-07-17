package com.learnactivemq.publisher.dto;

import jakarta.validation.constraints.NotBlank;

public record MessageRequest(@NotBlank String content) {
}
