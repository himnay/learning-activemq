package com.learnactivemq.publisher.controller;

import com.learnactivemq.publisher.dto.BulkPublishResponse;
import com.learnactivemq.publisher.dto.OrderRequest;
import com.learnactivemq.publisher.service.EventPublisherService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@Validated
@RestController
@RequiredArgsConstructor
@RequestMapping("/v1/events")
public class EventController {

    private final EventPublisherService publisherService;

    @PostMapping("/orders/bulk")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public BulkPublishResponse publishOrders(@Valid @RequestBody OrderRequest request,
                                                 @RequestParam(defaultValue = "100") @Min(1) @Max(1000) int count) {
        return publisherService.publishOrders(request, count);
    }
}
