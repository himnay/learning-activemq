package com.learnactivemq.publisher.service;

import com.learnactivemq.common.event.OrderCreatedEvent;
import com.learnactivemq.common.event.PaymentReceivedEvent;
import com.learnactivemq.common.event.ShipmentDispatchedEvent;
import com.learnactivemq.publisher.dto.OrderRequest;
import com.learnactivemq.publisher.dto.PaymentRequest;
import com.learnactivemq.publisher.dto.PublishResponse;
import com.learnactivemq.publisher.dto.ShipmentRequest;
import java.time.Instant;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class EventPublisherService {

    private final JmsTemplate jmsTemplate;
    private final String ordersTopic;
    private final String paymentsTopic;
    private final String shipmentsTopic;

    public EventPublisherService(JmsTemplate jmsTemplate,
                                 @Value("${app.topics.orders}") String ordersTopic,
                                 @Value("${app.topics.payments}") String paymentsTopic,
                                 @Value("${app.topics.shipments}") String shipmentsTopic) {
        this.jmsTemplate = jmsTemplate;
        this.ordersTopic = ordersTopic;
        this.paymentsTopic = paymentsTopic;
        this.shipmentsTopic = shipmentsTopic;
    }

    public PublishResponse publishOrderCreated(OrderRequest request) {
        OrderCreatedEvent event = new OrderCreatedEvent(
                UUID.randomUUID().toString(), request.product(), request.quantity(),
                request.amount(), Instant.now());
        return publish(ordersTopic, "order-created", event);
    }

    public PublishResponse publishPaymentReceived(PaymentRequest request) {
        PaymentReceivedEvent event = new PaymentReceivedEvent(
                UUID.randomUUID().toString(), request.orderId(), request.amount(),
                request.method(), Instant.now());
        return publish(paymentsTopic, "payment-received", event);
    }

    public PublishResponse publishShipmentDispatched(ShipmentRequest request) {
        ShipmentDispatchedEvent event = new ShipmentDispatchedEvent(
                UUID.randomUUID().toString(), request.orderId(), request.carrier(),
                request.trackingNumber(), Instant.now());
        return publish(shipmentsTopic, "shipment-dispatched", event);
    }

    private PublishResponse publish(String topic, String eventType, Object event) {
        String messageId = UUID.randomUUID().toString();
        jmsTemplate.convertAndSend(topic, event, message -> {
            message.setStringProperty("messageId", messageId);
            return message;
        });
        log.info("Published {} id={} to topic={}", eventType, messageId, topic);
        return new PublishResponse(messageId, eventType, topic, Instant.now());
    }
}
