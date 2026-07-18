package com.learnactivemq.publisher.jms;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import java.time.Instant;
import org.springframework.jms.core.MessagePostProcessor;

/**
 * Stamps outgoing events with custom JMS properties — the JMS equivalent of
 * Kafka producer headers. Runs after the converter builds the Message and
 * before JmsTemplate hands it to the producer, so payload and metadata stay
 * separated: properties are broker-visible (usable in selectors, shown in the
 * web console) without touching the JSON body.
 *
 * <pre>
 * jmsTemplate.convertAndSend(topic, event, new EventHeaderPostProcessor("order-created", messageId));
 * </pre>
 */
public class EventHeaderPostProcessor implements MessagePostProcessor {

    private final String eventType;
    private final String messageId;
    private final Integer seq;

    public EventHeaderPostProcessor(String eventType, String messageId) {
        this(eventType, messageId, null);
    }

    public EventHeaderPostProcessor(String eventType, String messageId, Integer seq) {
        this.eventType = eventType;
        this.messageId = messageId;
        this.seq = seq;
    }

    @Override
    public Message postProcessMessage(Message message) throws JMSException {
        message.setStringProperty("messageId", messageId);
        message.setStringProperty("eventType", eventType);
        message.setStringProperty("source", "activemq-publisher");
        message.setLongProperty("publishedAtEpochMs", Instant.now().toEpochMilli());
        if (seq != null) {
            message.setIntProperty("seq", seq);
        }
        return message;
    }
}
