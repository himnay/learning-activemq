package com.learnactivemq.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.learnactivemq.common.event.OrderCreatedEvent;
import com.learnactivemq.common.event.OrderQuoteReply;
import com.learnactivemq.common.event.OrderQuoteRequest;
import com.learnactivemq.consumer.support.FixtureLoader;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Message;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;
import tools.jackson.databind.ObjectMapper;

/**
 * Publishes multiple messages (loaded from fixtures/order-messages-*.json) at
 * an in-JVM ActiveMQ broker and asserts the real listener beans in this
 * Spring context (OrderCreatedEventListeners, OrderWorkerListeners,
 * DurableOrderListener, QuoteRequestListener) actually process them —
 * OrderCreatedEvent fan-out is proven with an independent topic probe,
 * request-reply is proven against the real QuoteRequestListener's reply,
 * for both the approved (positive) and rejected (negative) quote fixtures.
 */
@SpringBootTest
@ActiveProfiles("test")
@Import(OrderMessagingIntegrationTest.TestSupportConfig.class)
class OrderMessagingIntegrationTest {

    @Autowired
    private JmsTemplate jmsTemplate;

    @Autowired
    @Qualifier("testQueueJmsTemplate")
    private JmsTemplate queueJmsTemplate;

    @Autowired
    private MessageConverter messageConverter;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OrderCreatedTopicProbe topicProbe;

    @Value("${app.topics.virtual-orders}")
    private String virtualOrdersTopic;

    @Value("${app.queues.quote}")
    private String quoteQueue;

    private FixtureLoader positiveFixtures;
    private FixtureLoader negativeFixtures;

    @BeforeEach
    void loadFixtures() {
        positiveFixtures = new FixtureLoader(objectMapper, "fixtures/order-messages-positive.json");
        negativeFixtures = new FixtureLoader(objectMapper, "fixtures/order-messages-negative.json");
        topicProbe.received.clear();
    }

    @Test
    void publishedOrderCreatedEvents_areFannedOutToTopicSubscribers() {
        List<OrderCreatedEvent> events = positiveFixtures.ofType("order-created", OrderCreatedEvent.class);
        assertThat(events).isNotEmpty();

        events.forEach(event -> jmsTemplate.convertAndSend(virtualOrdersTopic, event));

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(topicProbe.received).hasSize(events.size()));

        assertThat(topicProbe.received)
                .extracting(OrderCreatedEvent::orderId)
                .containsExactlyInAnyOrderElementsOf(events.stream().map(OrderCreatedEvent::orderId).toList());
    }

    @Test
    void quoteRequestsWithinApprovalLimit_areApprovedByTheRealListener() throws Exception {
        List<OrderQuoteRequest> requests = positiveFixtures.ofType("order-quote-request", OrderQuoteRequest.class);
        assertThat(requests).isNotEmpty();
        assertQuoteReplies(requests, true);
    }

    @Test
    void quoteRequestsExceedingApprovalLimit_areRejectedByTheRealListener() throws Exception {
        List<OrderQuoteRequest> requests = negativeFixtures.ofType("order-quote-request", OrderQuoteRequest.class);
        assertThat(requests).isNotEmpty();
        assertQuoteReplies(requests, false);
    }

    private void assertQuoteReplies(List<OrderQuoteRequest> requests, boolean expectedApproved) throws Exception {
        for (OrderQuoteRequest request : requests) {
            String correlationId = UUID.randomUUID().toString();

            Message reply = queueJmsTemplate.sendAndReceive(quoteQueue, session -> {
                Message message = messageConverter.toMessage(request, session);
                message.setJMSCorrelationID(correlationId);
                return message;
            });

            assertThat(reply).as("reply for product=%s", request.product()).isNotNull();
            assertThat(reply.getJMSCorrelationID()).isEqualTo(correlationId);

            OrderQuoteReply quoteReply = (OrderQuoteReply) messageConverter.fromMessage(reply);
            BigDecimal expectedTotal = request.unitPrice().multiply(BigDecimal.valueOf(request.quantity()));

            assertThat(quoteReply.totalPrice()).isEqualByComparingTo(expectedTotal);
            assertThat(quoteReply.approved()).as("approved for product=%s", request.product())
                    .isEqualTo(expectedApproved);
        }
    }

    /** Collects OrderCreatedEvents seen on the virtual topic, independent of the app's own listeners. */
    @Component
    static class OrderCreatedTopicProbe {
        private final List<OrderCreatedEvent> received = new CopyOnWriteArrayList<>();

        @JmsListener(destination = "${app.topics.virtual-orders}")
        public void onEvent(OrderCreatedEvent event) {
            received.add(event);
        }
    }

    @EnableJms
    static class TestSupportConfig {

        @Bean
        OrderCreatedTopicProbe orderCreatedTopicProbe() {
            return new OrderCreatedTopicProbe();
        }

        /**
         * Defining any JmsTemplate bean switches off Boot's auto-configured one
         * (@ConditionalOnMissingBean(JmsOperations)), so the topic-mode template
         * the app itself relies on must be redeclared here alongside the
         * queue-mode one the quote test needs — same trap the publisher module's
         * QueueJmsConfig documents.
         */
        @Bean
        @Primary
        JmsTemplate jmsTemplate(ConnectionFactory connectionFactory, MessageConverter messageConverter) {
            JmsTemplate template = new JmsTemplate(connectionFactory);
            template.setMessageConverter(messageConverter);
            template.setPubSubDomain(true);
            return template;
        }

        /** Queue-mode template for the quote request/reply test — the topic template above is pub-sub. */
        @Bean
        JmsTemplate testQueueJmsTemplate(ConnectionFactory connectionFactory, MessageConverter messageConverter) {
            JmsTemplate template = new JmsTemplate(connectionFactory);
            template.setMessageConverter(messageConverter);
            template.setPubSubDomain(false);
            template.setReceiveTimeout(5000);
            return template;
        }
    }
}
