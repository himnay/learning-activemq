package com.learnactivemq.common.event;

import java.math.BigDecimal;

public record OrderQuoteReply(
        boolean approved,
        BigDecimal totalPrice,
        String note) {
}
