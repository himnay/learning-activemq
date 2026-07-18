package com.learnactivemq.consumer.listener;

import com.learnactivemq.common.event.OrderCreatedEvent;
import jakarta.jms.Destination;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.support.JmsHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Plain (non-durable) topic subscriber on VirtualTopic.orders — a virtual
 * topic is still a normal topic for direct subscribers, so this gets a copy
 * of every published event while connected.
 */
@Slf4j
@Component
public class OrderCreatedEventListeners {

    @JmsListener(destination = "${app.topics.virtual-orders}")
    public void onOrderCreated(OrderCreatedEvent event,
                               @Header(name = "messageId", required = false) String messageId,
                               @Header(JmsHeaders.DESTINATION) Destination destination) {
        log.info("Consumed order-created from={} id={} orderId={} product={} qty={} amount={}",
                destination, messageId, event.orderId(), event.product(), event.quantity(), event.amount());
    }
}
