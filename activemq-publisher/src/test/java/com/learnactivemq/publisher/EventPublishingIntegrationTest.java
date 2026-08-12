package com.learnactivemq.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

import com.learnactivemq.common.event.OrderCreatedEvent;
import com.learnactivemq.common.event.OrderQuoteReply;
import com.learnactivemq.common.event.OrderQuoteRequest;
import com.learnactivemq.publisher.dto.BulkPublishResponse;
import com.learnactivemq.publisher.dto.OrderRequest;
import com.learnactivemq.publisher.support.FixtureLoader;
import com.learnactivemq.publisher.support.MessageFixture;
import jakarta.jms.ConnectionFactory;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jms.autoconfigure.DefaultJmsListenerContainerFactoryConfigurer;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;
import tools.jackson.databind.ObjectMapper;

/**
 * Drives the real REST endpoints (EventController, QuoteController) over HTTP
 * against an in-JVM broker, then verifies the messages actually landed:
 * OrderCreatedEvents are observed by an independent topic probe, quote
 * requests are answered by a stub responder standing in for the consumer
 * module's QuoteRequestListener (out of process in a real deployment), and
 * the negative fixtures exercise both a real business rejection (quote over
 * the approval limit) and real bean-validation failures (blank product,
 * quantity/count out of range) on EventController/QuoteController.
 */
@ActiveProfiles("test")
@AutoConfigureTestRestTemplate
@SpringBootTest(webEnvironment = RANDOM_PORT)
@Import(EventPublishingIntegrationTest.TestSupportConfig.class)
class EventPublishingIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OrderCreatedTopicProbe topicProbe;

    @Value("${app.topics.virtual-orders}")
    private String virtualOrdersTopic;

    private FixtureLoader positiveFixtures;
    private FixtureLoader negativeFixtures;

    @BeforeEach
    void loadFixtures() {
        positiveFixtures = new FixtureLoader(objectMapper, "fixtures/order-requests-positive.json");
        negativeFixtures = new FixtureLoader(objectMapper, "fixtures/order-requests-negative.json");
        topicProbe.received.clear();
    }

    @Test
    void bulkOrderRequests_arePublishedToTheVirtualTopic() {
        List<MessageFixture> bulkOrders = positiveFixtures.ofType("bulk-order");
        assertThat(bulkOrders).isNotEmpty();

        int expectedTotal = 0;
        for (MessageFixture fixture : bulkOrders) {
            OrderRequest request = positiveFixtures.payloadAs(fixture, OrderRequest.class);
            int count = fixture.count();
            expectedTotal += count;

            ResponseEntity<BulkPublishResponse> response = restTemplate.postForEntity(
                    "/v1/events/orders/bulk?count=" + count, request, BulkPublishResponse.class);

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().getCount()).isEqualTo(count);
            assertThat(response.getBody().getTopic()).isEqualTo(virtualOrdersTopic);
        }

        int finalExpectedTotal = expectedTotal;
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(topicProbe.received).hasSize(finalExpectedTotal));

        assertThat(topicProbe.received)
                .allSatisfy(event -> assertThat(event.orderId()).isNotBlank());
    }

    @Test
    void quoteRequestsWithinApprovalLimit_areApprovedThroughTheApi() {
        List<MessageFixture> quoteRequests = positiveFixtures.ofType("quote-request");
        assertThat(quoteRequests).isNotEmpty();
        assertQuoteReplies(positiveFixtures, quoteRequests, true);
    }

    @Test
    void quoteRequestsExceedingApprovalLimit_areRejectedButStillReturnOk() {
        List<MessageFixture> quoteRequests = negativeFixtures.ofType("quote-request-rejected");
        assertThat(quoteRequests).isNotEmpty();
        assertQuoteReplies(negativeFixtures, quoteRequests, false);
    }

    private void assertQuoteReplies(FixtureLoader loader, List<MessageFixture> quoteRequestFixtures,
                                     boolean expectedApproved) {
        for (MessageFixture fixture : quoteRequestFixtures) {
            OrderRequest request = loader.payloadAs(fixture, OrderRequest.class);

            ResponseEntity<OrderQuoteReply> response =
                    restTemplate.postForEntity("/v1/orders/quote", request, OrderQuoteReply.class);

            BigDecimal expectedTotal = request.getAmount().multiply(BigDecimal.valueOf(request.getQuantity()));

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(response.getBody()).isNotNull();
            assertThat(response.getBody().totalPrice()).isEqualByComparingTo(expectedTotal);
            assertThat(response.getBody().approved()).as("approved for product=%s", request.getProduct())
                    .isEqualTo(expectedApproved);
        }
    }

    @Test
    void invalidOrderPayloads_areRejectedWithBadRequest() {
        List<MessageFixture> invalidOrders = negativeFixtures.ofType("invalid-order");
        assertThat(invalidOrders).isNotEmpty();

        for (MessageFixture fixture : invalidOrders) {
            OrderRequest request = negativeFixtures.payloadAs(fixture, OrderRequest.class);

            ResponseEntity<String> quoteResponse =
                    restTemplate.postForEntity("/v1/orders/quote", request, String.class);
            assertThat(quoteResponse.getStatusCode()).as("quote status for payload=%s", request)
                    .isEqualTo(HttpStatus.BAD_REQUEST);

            ResponseEntity<String> bulkResponse = restTemplate.postForEntity(
                    "/v1/events/orders/bulk?count=1", request, String.class);
            assertThat(bulkResponse.getStatusCode()).as("bulk status for payload=%s", request)
                    .isEqualTo(HttpStatus.BAD_REQUEST);
        }
    }

    @Test
    void invalidBulkCount_isRejectedWithBadRequest() {
        List<MessageFixture> invalidCounts = negativeFixtures.ofType("invalid-bulk-count");
        assertThat(invalidCounts).isNotEmpty();

        for (MessageFixture fixture : invalidCounts) {
            OrderRequest request = negativeFixtures.payloadAs(fixture, OrderRequest.class);
            int count = fixture.count();

            ResponseEntity<String> response = restTemplate.postForEntity(
                    "/v1/events/orders/bulk?count=" + count, request, String.class);

            assertThat(response.getStatusCode()).as("status for count=%d", count)
                    .isEqualTo(HttpStatus.BAD_REQUEST);
        }
    }

    /** Collects OrderCreatedEvents seen on the virtual topic, independent of the publisher's own code. */
    @Component
    static class OrderCreatedTopicProbe {
        private final List<OrderCreatedEvent> received = new CopyOnWriteArrayList<>();

        @JmsListener(destination = "${app.topics.virtual-orders}")
        public void onEvent(OrderCreatedEvent event) {
            received.add(event);
        }
    }

    /** Stands in for the consumer module's QuoteRequestListener, which isn't running in this module's test. */
    @Component
    static class StubQuoteResponder {

        private static final BigDecimal APPROVAL_LIMIT = new BigDecimal("5000");

        @JmsListener(destination = "${app.queues.quote}", containerFactory = "testQueueListenerFactory")
        public OrderQuoteReply onQuoteRequest(OrderQuoteRequest request) {
            BigDecimal total = request.unitPrice().multiply(BigDecimal.valueOf(request.quantity()));
            boolean approved = total.compareTo(APPROVAL_LIMIT) <= 0;
            return new OrderQuoteReply(approved, total, approved ? "within approval limit" : "exceeds approval limit");
        }
    }

    @EnableJms
    static class TestSupportConfig {

        @Bean
        OrderCreatedTopicProbe orderCreatedTopicProbe() {
            return new OrderCreatedTopicProbe();
        }

        @Bean
        StubQuoteResponder stubQuoteResponder() {
            return new StubQuoteResponder();
        }

        /** Queue-mode factory for the quote responder — the app's default factory is topic-mode. */
        @Bean
        DefaultJmsListenerContainerFactory testQueueListenerFactory(
                ConnectionFactory connectionFactory, DefaultJmsListenerContainerFactoryConfigurer configurer) {
            DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
            configurer.configure(factory, connectionFactory);
            factory.setPubSubDomain(false);
            return factory;
        }
    }
}
