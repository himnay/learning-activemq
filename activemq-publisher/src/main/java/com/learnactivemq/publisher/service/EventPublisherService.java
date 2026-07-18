package com.learnactivemq.publisher.service;

import com.learnactivemq.common.event.OrderCreatedEvent;
import com.learnactivemq.publisher.dto.BulkPublishResponse;
import com.learnactivemq.publisher.dto.OrderRequest;
import com.learnactivemq.publisher.jms.EventHeaderPostProcessor;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;

/**
 * Publishes order-created events to the VirtualTopic.orders virtual topic.
 * One destination feeds every consumption pattern: direct topic subscribers
 * (plain + durable) get each event, and the broker also copies it into the
 * Consumer.*.VirtualTopic.orders worker queues for round-robin processing.
 */
@Slf4j
@Service
public class EventPublisherService {

    private final JmsTemplate jmsTemplate;
    private final String virtualOrdersTopic;

    public EventPublisherService(JmsTemplate jmsTemplate,
                                 @Value("${app.topics.virtual-orders}") String virtualOrdersTopic) {
        this.jmsTemplate = jmsTemplate;
        this.virtualOrdersTopic = virtualOrdersTopic;
    }

    /** Publishes orders. */
    public BulkPublishResponse publishOrders(OrderRequest request, int count) {
        for (int seq = 1; seq <= count; seq++) {
            OrderCreatedEvent event = new OrderCreatedEvent(
                    UUID.randomUUID().toString(), request.getProduct(), request.getQuantity(),
                    request.getAmount(), Instant.now());
            jmsTemplate.convertAndSend(virtualOrdersTopic, event,
                    new EventHeaderPostProcessor("order-created", UUID.randomUUID().toString(), seq));
        }
        log.info("Published burst of {} order-created events to topic={}", count, virtualOrdersTopic);
        return new BulkPublishResponse()
                .count(count)
                .topic(virtualOrdersTopic)
                .publishedAt(OffsetDateTime.now());
    }
}
