package com.learnactivemq.consumer.config;

import org.apache.activemq.RedeliveryPolicy;
import org.springframework.boot.activemq.autoconfigure.ActiveMQConnectionFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Client-side redelivery policy. Boot's listener sessions are transacted, so a
 * listener exception rolls the message back; the client then redelivers with
 * this schedule before giving up — at which point the broker moves the message
 * to its DLQ (per-queue DLQ.* here, see broker/activemq.xml).
 *
 * Schedule: 0.5s → 1s → 2s, then dead-lettered (3 redeliveries + original try).
 */
@Configuration
public class RedeliveryConfig {

    /** Defines the redelivery policy customizer bean. */
    @Bean
    public ActiveMQConnectionFactoryCustomizer redeliveryPolicyCustomizer() {
        return factory -> {
            RedeliveryPolicy policy = factory.getRedeliveryPolicy();
            policy.setMaximumRedeliveries(3);
            policy.setInitialRedeliveryDelay(500);
            policy.setUseExponentialBackOff(true);
            policy.setBackOffMultiplier(2.0);
        };
    }
}
