package com.learnactivemq.consumer.listener;

import com.learnactivemq.common.event.OrderQuoteReply;
import com.learnactivemq.common.event.OrderQuoteRequest;
import jakarta.jms.Destination;
import java.math.BigDecimal;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.support.JmsHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Responder side of request-reply. Returning a value from a @JmsListener
 * method makes Spring send it to the request's JMSReplyTo destination and
 * copy the JMSCorrelationID — no manual reply plumbing.
 */
@Slf4j
@Component
public class QuoteRequestListener {

    private static final BigDecimal APPROVAL_LIMIT = new BigDecimal("5000");

    /** Handles quote request. */
    @JmsListener(destination = "${app.queues.quote}", containerFactory = "queueListenerFactory")
    public OrderQuoteReply onQuoteRequest(OrderQuoteRequest request,
                                          @Header(JmsHeaders.CORRELATION_ID) String correlationId,
                                          @Header(JmsHeaders.REPLY_TO) Destination replyTo) {
        BigDecimal total = request.unitPrice().multiply(BigDecimal.valueOf(request.quantity()));
        boolean approved = total.compareTo(APPROVAL_LIMIT) <= 0;

        log.info("Quote request correlationId={} product={} qty={} total={} -> approved={} replyTo={}",
                correlationId, request.product(), request.quantity(), total, approved, replyTo);

        return new OrderQuoteReply(approved, total,
                approved ? "within approval limit" : "exceeds approval limit " + APPROVAL_LIMIT);
    }
}
