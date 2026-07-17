package com.learnactivemq.publisher.controller;

import com.learnactivemq.publisher.dto.BulkPublishResponse;
import com.learnactivemq.publisher.dto.OrderRequest;
import com.learnactivemq.publisher.dto.PaymentRequest;
import com.learnactivemq.publisher.dto.PublishResponse;
import com.learnactivemq.publisher.dto.ShipmentRequest;
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
@RequestMapping("/v1/events")
@RequiredArgsConstructor
public class EventController {

    private final EventPublisherService publisherService;

    @PostMapping("/orders")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PublishResponse publishOrder(@Valid @RequestBody OrderRequest request) {
        return publisherService.publishOrderCreated(request);
    }

    @PostMapping("/orders/bulk")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public BulkPublishResponse publishOrderBurst(@Valid @RequestBody OrderRequest request,
                                                 @RequestParam(defaultValue = "100") @Min(1) @Max(1000) int count) {
        return publisherService.publishOrderBurst(request, count);
    }

    @PostMapping("/payments")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PublishResponse publishPayment(@Valid @RequestBody PaymentRequest request) {
        return publisherService.publishPaymentReceived(request);
    }

    @PostMapping("/shipments")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public PublishResponse publishShipment(@Valid @RequestBody ShipmentRequest request) {
        return publisherService.publishShipmentDispatched(request);
    }
}
