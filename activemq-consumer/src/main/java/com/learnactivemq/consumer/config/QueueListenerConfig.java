package com.learnactivemq.consumer.config;

import jakarta.jms.ConnectionFactory;
import org.springframework.boot.jms.autoconfigure.DefaultJmsListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;

/**
 * The default factory is topic-mode (spring.jms.pub-sub-domain: true). The
 * virtual-topic worker queues need a queue-mode factory with competing
 * consumers — that's where the broker round-robins messages.
 */
@Configuration
public class QueueListenerConfig {

    @Bean
    public DefaultJmsListenerContainerFactory queueListenerFactory(
            ConnectionFactory connectionFactory,
            DefaultJmsListenerContainerFactoryConfigurer configurer) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        factory.setPubSubDomain(false);
        factory.setConcurrency("3-3");
        return factory;
    }
}
