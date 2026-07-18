package com.learnactivemq.publisher.controller;

import com.learnactivemq.common.event.OrderQuoteReply;
import com.learnactivemq.publisher.dto.OrderRequest;
import com.learnactivemq.publisher.service.QuoteService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/v1/orders")
public class QuoteController {

    private final QuoteService quoteService;

    @PostMapping("/quote")
    public ResponseEntity<OrderQuoteReply> quote(@Valid @RequestBody OrderRequest request)
            throws Exception {
        OrderQuoteReply reply = quoteService.requestQuote(request);
        if (reply == null) {
            return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT).build();
        }
        return ResponseEntity.ok(reply);
    }
}
