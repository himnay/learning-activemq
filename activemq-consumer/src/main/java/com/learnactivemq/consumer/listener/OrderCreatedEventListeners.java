package com.learnactivemq.consumer.listener;

import com.learnactivemq.common.event.OrderCreatedEvent;
import com.learnactivemq.common.event.PaymentReceivedEvent;
import com.learnactivemq.common.event.ShipmentDispatchedEvent;
import jakarta.jms.Destination;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.support.JmsHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderCreatedEventListeners {

    @JmsListener(destination = "${app.topics.orders}")
    public void onOrderCreated(OrderCreatedEvent event,
                               @Header(name = "messageId", required = false) String messageId,
                               @Header(JmsHeaders.DESTINATION) Destination destination) {
        log.info("Consumed order-created from={} id={} orderId={} product={} qty={} amount={}",
                destination, messageId, event.orderId(), event.product(), event.quantity(), event.amount());
    }

    @JmsListener(destination = "${app.topics.payments}")
    public void onPaymentReceived(PaymentReceivedEvent event,
                                  @Header(name = "messageId", required = false) String messageId,
                                  @Header(JmsHeaders.DESTINATION) Destination destination) {
        log.info("Consumed payment-received from={} id={} paymentId={} orderId={} amount={} method={}",
                destination, messageId, event.paymentId(), event.orderId(), event.amount(), event.method());
    }

    @JmsListener(destination = "${app.topics.shipments}")
    public void onShipmentDispatched(ShipmentDispatchedEvent event,
                                     @Header(name = "messageId", required = false) String messageId,
                                     @Header(JmsHeaders.DESTINATION) Destination destination) {
        log.info("Consumed shipment-dispatched from={} id={} shipmentId={} orderId={} carrier={} tracking={}",
                destination, messageId, event.shipmentId(), event.orderId(), event.carrier(), event.trackingNumber());
    }
}
