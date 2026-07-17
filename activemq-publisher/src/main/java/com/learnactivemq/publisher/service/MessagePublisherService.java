package com.learnactivemq.publisher.service;

import com.learnactivemq.publisher.dto.MessageRequest;
import com.learnactivemq.publisher.dto.MessageResponse;
import java.time.Instant;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MessagePublisherService {

    private final JmsTemplate jmsTemplate;
    private final String queue;

    public MessagePublisherService(JmsTemplate jmsTemplate, @Value("${app.queue}") String queue) {
        this.jmsTemplate = jmsTemplate;
        this.queue = queue;
    }

    public MessageResponse publish(MessageRequest request) {
        String messageId = UUID.randomUUID().toString();
        jmsTemplate.convertAndSend(queue, request.content(), message -> {
            message.setStringProperty("messageId", messageId);
            return message;
        });
        log.info("Published message id={} to queue={}", messageId, queue);
        return new MessageResponse(messageId, queue, Instant.now());
    }
}
