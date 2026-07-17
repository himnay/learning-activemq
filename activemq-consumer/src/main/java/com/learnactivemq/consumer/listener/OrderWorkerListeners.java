package com.learnactivemq.consumer.listener;

import com.learnactivemq.common.event.OrderCreatedEvent;
import jakarta.jms.Destination;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.support.JmsHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Round-robin demo. The publisher sends bulk orders to the VirtualTopic.orders
 * topic; the broker mirrors every message into each Consumer.*.VirtualTopic.orders
 * queue (fan-out), and inside a queue the 3 competing consumers (queueListenerFactory
 * concurrency) receive messages round-robin — watch the seq/thread in the log.
 */
@Slf4j
@Component
public class OrderWorkerListeners {

    @JmsListener(destination = "${app.queues.worker-a}", containerFactory = "queueListenerFactory")
    public void onWorkerA(OrderCreatedEvent event,
                          @Header(name = "seq", required = false) Integer seq,
                          @Header(JmsHeaders.DESTINATION) Destination destination) {
        log.info("workerA consumed from={} seq={} orderId={} thread={}",
                destination, seq, event.orderId(), Thread.currentThread().getName());
    }

    @JmsListener(destination = "${app.queues.worker-b}", containerFactory = "queueListenerFactory")
    public void onWorkerB(OrderCreatedEvent event,
                          @Header(name = "seq", required = false) Integer seq,
                          @Header(JmsHeaders.DESTINATION) Destination destination) {
        log.info("workerB consumed from={} seq={} orderId={} thread={}",
                destination, seq, event.orderId(), Thread.currentThread().getName());
    }
}
