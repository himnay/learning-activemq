package com.learnactivemq.common.event;

import java.math.BigDecimal;

public record OrderQuoteRequest(
        String product,
        int quantity,
        BigDecimal unitPrice) {
}
