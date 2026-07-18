package com.learnactivemq.common.config;

import com.learnactivemq.common.event.OrderCreatedEvent;
import com.learnactivemq.common.event.OrderQuoteReply;
import com.learnactivemq.common.event.OrderQuoteRequest;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.support.converter.JacksonJsonMessageConverter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.MessageType;

/**
 * Serializes events as JSON text messages. The {@code _event} type-id header
 * (not the class name) selects the target type on the consumer side, so both
 * modules stay decoupled from each other's packages.
 */
@Configuration
public class JmsEventConverterConfig {

    public static final String TYPE_ID_PROPERTY = "_event";

    @Bean
    public MessageConverter jacksonJmsMessageConverter() {
        JacksonJsonMessageConverter converter = new JacksonJsonMessageConverter();
        converter.setTargetType(MessageType.TEXT);
        converter.setTypeIdPropertyName(TYPE_ID_PROPERTY);
        converter.setTypeIdMappings(Map.of(
                "order-created", OrderCreatedEvent.class,
                "order-quote-request", OrderQuoteRequest.class,
                "order-quote-reply", OrderQuoteReply.class));
        return converter;
    }
}
