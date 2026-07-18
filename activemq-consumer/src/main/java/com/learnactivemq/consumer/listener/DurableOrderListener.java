package com.learnactivemq.consumer.listener;

import com.learnactivemq.common.event.OrderCreatedEvent;
import jakarta.jms.Destination;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.support.JmsHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Durable subscription demo on the VirtualTopic.orders topic. Unlike the
 * plain subscriber in OrderCreatedEventListeners (which misses anything
 * published while the app is down), the broker stores messages for clientId
 * "activemq-consumer" + subscription "orders-durable-sub" and replays them on
 * reconnect. Stop the consumer, publish orders, start it again — this
 * listener catches up, the plain one stays silent.
 */
@Slf4j
@Component
public class DurableOrderListener {

    @JmsListener(destination = "${app.topics.virtual-orders}",
            containerFactory = "durableTopicListenerFactory",
            subscription = "${app.subscriptions.orders-durable}")
    public void onOrderDurable(OrderCreatedEvent event,
                               @Header(name = "messageId", required = false) String messageId,
                               @Header(JmsHeaders.DESTINATION) Destination destination) {
        log.info("DURABLE consumed order-created from={} id={} orderId={} product={} qty={}",
                destination, messageId, event.orderId(), event.product(), event.quantity());
    }
}
