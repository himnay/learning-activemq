package com.learnactivemq.publisher.controller;

import com.learnactivemq.publisher.dto.MessageRequest;
import com.learnactivemq.publisher.dto.MessageResponse;
import com.learnactivemq.publisher.service.MessagePublisherService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/messages")
@RequiredArgsConstructor
public class MessageController {

    private final MessagePublisherService publisherService;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public MessageResponse publish(@Valid @RequestBody MessageRequest request) {
        return publisherService.publish(request);
    }
}
