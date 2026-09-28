package com.learnactivemq.consumer.config;

import jakarta.jms.ConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.activemq.autoconfigure.ActiveMQConnectionDetails;
import org.springframework.boot.activemq.autoconfigure.ActiveMQConnectionFactoryCustomizer;
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

    /** JMS clientId of the durable subscriber's connection (half of the subscription's identity). */
    public static final String DURABLE_CLIENT_ID = "activemq-consumer";

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
     * bean would switch off Boot's auto-configured one — so Boot's connection
     * details and ActiveMQConnectionFactoryCustomizers (RedeliveryConfig) are
     * applied to it by hand. The listener container sets the clientId on the
     * one connection it opens and closes that connection on shutdown.
     */
    @Bean
    public DefaultJmsListenerContainerFactory durableTopicListenerFactory(
            DefaultJmsListenerContainerFactoryConfigurer configurer,
            ActiveMQConnectionDetails connectionDetails,
            ObjectProvider<ActiveMQConnectionFactoryCustomizer> customizers) {
        ActiveMQConnectionFactory dedicated = new ActiveMQConnectionFactory(
                connectionDetails.getUser(), connectionDetails.getPassword(), connectionDetails.getBrokerUrl());
        customizers.orderedStream().forEach(customizer -> customizer.customize(dedicated));

        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        configurer.configure(factory, dedicated);
        factory.setClientId(DURABLE_CLIENT_ID);
        factory.setPubSubDomain(true);
        factory.setSubscriptionDurable(true);
        return factory;
    }
}
