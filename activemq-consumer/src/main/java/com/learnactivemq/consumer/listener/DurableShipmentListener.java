package com.learnactivemq.consumer.listener;

import com.learnactivemq.common.event.ShipmentDispatchedEvent;
import jakarta.jms.Destination;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.support.JmsHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Durable subscription demo on shipments.topic. Unlike the plain subscriber in
 * EventListeners (which misses anything published while the app is down), the
 * broker stores messages for clientId "activemq-consumer" + subscription
 * "shipments-durable-sub" and replays them on reconnect. Stop the consumer,
 * publish shipments, start it again — this listener catches up, the plain one
 * stays silent.
 */
@Slf4j
@Component
public class DurableShipmentListener {

    @JmsListener(destination = "${app.topics.shipments}",
            containerFactory = "durableTopicListenerFactory",
            subscription = "${app.subscriptions.shipments-durable}")
    public void onShipmentDurable(ShipmentDispatchedEvent event,
                                  @Header(name = "messageId", required = false) String messageId,
                                  @Header(JmsHeaders.DESTINATION) Destination destination) {
        log.info("DURABLE consumed shipment-dispatched from={} id={} shipmentId={} carrier={} tracking={}",
                destination, messageId, event.shipmentId(), event.carrier(), event.trackingNumber());
    }
}
