package com.learnactivemq.publisher.config;

import jakarta.jms.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MessageConverter;

/**
 * Two templates: the primary one publishes events to topics; the queue one
 * exists for request-reply, which needs queue semantics — the request must
 * land on exactly one responder, and sendAndReceive() waits on a temporary
 * reply queue.
 *
 * Defining any JmsTemplate bean switches off Boot's auto-configured one
 * (@ConditionalOnMissingBean(JmsOperations)), so the topic template must be
 * declared explicitly here and marked @Primary.
 */
@Configuration
public class QueueJmsConfig {

    /** Defines the jms template bean. */
    @Bean
    @Primary
    public JmsTemplate jmsTemplate(ConnectionFactory connectionFactory,
                                   MessageConverter messageConverter) {
        JmsTemplate template = new JmsTemplate(connectionFactory);
        template.setMessageConverter(messageConverter);
        template.setPubSubDomain(true);
        return template;
    }

    /** Defines the queue jms template bean. */
    @Bean
    public JmsTemplate queueJmsTemplate(ConnectionFactory connectionFactory,
                                        MessageConverter messageConverter) {
        JmsTemplate template = new JmsTemplate(connectionFactory);
        template.setMessageConverter(messageConverter);
        template.setPubSubDomain(false);
        template.setReceiveTimeout(5000);   // give up if no responder answers in 5s
        return template;
    }
}
