package com.learnactivemq.consumer.listener;

import com.learnactivemq.common.event.OrderCreatedEvent;
import jakarta.jms.Destination;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.support.JmsHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Round-robin demo. The publisher sends bulk orders to the VirtualTopic.orders
 * topic; the broker mirrors every message into each Consumer.*.VirtualTopic.orders
 * queue (fan-out), and inside a queue the 3 competing consumers (queueListenerFactory
 * concurrency) receive messages round-robin — watch the seq/thread in the log.
 *
 * DLQ demo: with app.listener.fail-seq-multiple > 0, whichever worker receives a
 * seq divisible by it throws — the message is redelivered per RedeliveryConfig's
 * backoff schedule, then dead-lettered to DLQ.Consumer.workers.VirtualTopic.orders.
 * (Both workers must apply the switch: they share one queue, so round-robin would
 * otherwise route every even seq to workerB and the demo would never fire.)
 */
@Slf4j
@Component
public class OrderWorkerListeners {

    private final int failSeqMultiple;

    public OrderWorkerListeners(@Value("${app.listener.fail-seq-multiple:0}") int failSeqMultiple) {
        this.failSeqMultiple = failSeqMultiple;
    }

    /** Handles worker a. */
    @JmsListener(destination = "${app.queues.worker-a}", containerFactory = "queueListenerFactory",
            concurrency = "${app.listener.worker-concurrency}")
    public void onWorkerA(OrderCreatedEvent event,
                          @Header(name = "seq", required = false) Integer seq,
                          @Header(JmsHeaders.DESTINATION) Destination destination) {
        log.info("workerA consumed from={} seq={} orderId={} thread={}",
                destination, seq, event.orderId(), Thread.currentThread().getName());
        failIfPoison("workerA", seq);
    }

    /** Handles worker b. */
    @JmsListener(destination = "${app.queues.worker-b}", containerFactory = "queueListenerFactory",
            concurrency = "${app.listener.worker-concurrency}")
    public void onWorkerB(OrderCreatedEvent event,
                          @Header(name = "seq", required = false) Integer seq,
                          @Header(JmsHeaders.DESTINATION) Destination destination) {
        log.info("workerB consumed from={} seq={} orderId={} thread={}",
                destination, seq, event.orderId(), Thread.currentThread().getName());
        failIfPoison("workerB", seq);
    }

    private void failIfPoison(String worker, Integer seq) {
        if (failSeqMultiple > 0 && seq != null && seq % failSeqMultiple == 0) {
            log.warn("{} SIMULATED FAILURE seq={} — rolling back for redelivery", worker, seq);
            throw new IllegalStateException("simulated failure for seq=" + seq);
        }
    }
}
