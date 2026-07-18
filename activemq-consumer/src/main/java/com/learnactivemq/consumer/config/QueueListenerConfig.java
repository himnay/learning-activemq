package com.learnactivemq.consumer.config;

import jakarta.jms.ConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jms.autoconfigure.DefaultJmsListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;

/**
 * The default factory is topic-mode (spring.jms.pub-sub-domain: true). The
 * virtual-topic worker queues need a queue-mode factory — concurrency comes
 * from each @JmsListener via app.listener.worker-concurrency in application.yml.
 */
@Configuration
public class QueueListenerConfig {

    /** Defines the queue listener factory bean. */
    @Bean
    public DefaultJmsListenerContainerFactory queueListenerFactory(
            ConnectionFactory connectionFactory,
            DefaultJmsListenerContainerFactoryConfigurer configurer) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        factory.setPubSubDomain(false);
        return factory;
    }

    /**
     * Durable topic subscription factory. clientId + subscription name identify
     * the subscription on the broker — events published while this consumer is
     * offline are stored and replayed on reconnect.
     *
     * Needs its own connection: a clientId must be set before the connection
     * starts, which Boot's shared CachingConnectionFactory forbids. The private
     * factory below is intentionally NOT a bean — a second ConnectionFactory
     * bean would switch off Boot's auto-configured one.
     */
    @Bean
    public DefaultJmsListenerContainerFactory durableTopicListenerFactory(
            DefaultJmsListenerContainerFactoryConfigurer configurer,
            @Value("${spring.activemq.broker-url}") String brokerUrl,
            @Value("${spring.activemq.user}") String user,
            @Value("${spring.activemq.password}") String password) {
        ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(user, password, brokerUrl);
        CachingConnectionFactory dedicated = new CachingConnectionFactory(amq);
        dedicated.setClientId("activemq-consumer");

        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        configurer.configure(factory, dedicated);
        factory.setPubSubDomain(true);
        factory.setSubscriptionDurable(true);
        return factory;
    }
}
