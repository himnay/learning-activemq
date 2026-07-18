package com.learnactivemq.consumer.config;

import jakarta.jms.ConnectionFactory;
import org.springframework.boot.jms.autoconfigure.DefaultJmsListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;

/**
 * The default factory is topic-mode (spring.jms.pub-sub-domain: true). The
 * virtual-topic worker queues need a queue-mode factory — concurrency comes
 * from each @JmsListener via app.listener.worker-concurrency in application.yml.
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
        return factory;
    }
}
