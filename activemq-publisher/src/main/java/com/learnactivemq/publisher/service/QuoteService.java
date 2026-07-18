package com.learnactivemq.publisher.service;

import com.learnactivemq.common.event.OrderQuoteReply;
import com.learnactivemq.common.event.OrderQuoteRequest;
import com.learnactivemq.publisher.dto.OrderRequest;
import jakarta.jms.Message;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.stereotype.Service;

/**
 * Request-reply over JMS. sendAndReceive() creates a temporary reply queue,
 * stamps it as JMSReplyTo on the request, then blocks until the responder's
 * reply arrives there. JMSCorrelationID ties the pair together in the logs.
 */
@Slf4j
@Service
public class QuoteService {

    private final JmsTemplate queueJmsTemplate;
    private final MessageConverter messageConverter;
    private final String quoteQueue;

    public QuoteService(@Qualifier("queueJmsTemplate") JmsTemplate queueJmsTemplate,
                        MessageConverter messageConverter,
                        @Value("${app.queues.quote}") String quoteQueue) {
        this.queueJmsTemplate = queueJmsTemplate;
        this.messageConverter = messageConverter;
        this.quoteQueue = quoteQueue;
    }

    /** Requests quote. */
    public OrderQuoteReply requestQuote(OrderRequest request) throws Exception {
        String correlationId = UUID.randomUUID().toString();
        OrderQuoteRequest quoteRequest =
                new OrderQuoteRequest(request.getProduct(), request.getQuantity(), request.getAmount());

        log.info("Quote request correlationId={} product={} qty={}",
                correlationId, request.getProduct(), request.getQuantity());

        Message reply = queueJmsTemplate.sendAndReceive(quoteQueue, session -> {
            Message message = messageConverter.toMessage(quoteRequest, session);
            message.setJMSCorrelationID(correlationId);
            return message;
        });

        if (reply == null) {
            log.warn("Quote request correlationId={} timed out", correlationId);
            return null;
        }
        log.info("Quote reply received correlationId={}", reply.getJMSCorrelationID());
        return (OrderQuoteReply) messageConverter.fromMessage(reply);
    }
}
