package com.courier.worker.consumer;

import com.courier.common.dto.NotificationEvent;
import com.courier.common.enums.Channel;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.repository.NotificationRepository;
import com.courier.worker.delivery.DeliveryService;
import com.courier.worker.service.DeduplicationService;
import com.courier.worker.service.StatusTrackingService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/*
 * @EmbeddedKafka: starts an in-memory Kafka broker for this test class.
 * partitions=1 keeps it simple for tests - we don't need parallelism here.
 * topics declares the topics that must exist before any test runs.
 *
 * @DirtiesContext: resets the Spring context after this test class.
 * EmbeddedKafka modifies Kafka configuration - without this, other
 * test classes that also use Kafka could pick up stale config.
 */
@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"courier.email", "courier.sms", "courier.webhook", "courier.dlq"}
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@Import(NotificationConsumerTestConfig.class)
@DisplayName("NotificationConsumer Integration Tests")
class NotificationConsumerTest {

    @Autowired
    private KafkaTemplate<String, NotificationEvent> kafkaTemplate;

    @MockBean
    private DeduplicationService deduplicationService;

    @MockBean
    private StatusTrackingService statusTrackingService;

    @MockBean
    private DeliveryService deliveryService;

    @MockBean
    private NotificationRepository notificationRepository;

    @Test
    @DisplayName("Should process message and update status to DELIVERED on success")
    void shouldProcessMessageAndDeliverSuccessfully() throws Exception {
        // Given: not a duplicate, delivery succeeds
        when(deduplicationService.isDuplicate(any())).thenReturn(false);
        doNothing().when(deliveryService).deliver(any());

        NotificationEvent event = buildEvent(Channel.EMAIL);

        // When: publish to Kafka
        kafkaTemplate.send("courier.email", event.getCorrelationId(), event).get();

        // Then: wait for async consumer to process
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    verify(deduplicationService).isDuplicate(event.getEventId());
                    verify(statusTrackingService).updateStatus(
                            eq(event.getNotificationId()),
                            eq(NotificationStatus.PROCESSING));
                    verify(deliveryService).deliver(any());
                    verify(statusTrackingService).updateStatus(
                            eq(event.getNotificationId()),
                            eq(NotificationStatus.DELIVERED));
                });
    }

    @Test
    @DisplayName("Should skip processing and ack when event is a duplicate")
    void shouldSkipDuplicateEvent() throws Exception {
        // Given: deduplication returns true (duplicate)
        when(deduplicationService.isDuplicate(any())).thenReturn(true);

        NotificationEvent event = buildEvent(Channel.EMAIL);

        // When
        kafkaTemplate.send("courier.email", event.getCorrelationId(), event).get();

        // Then: delivery should never be called for duplicates
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        verify(deduplicationService)
                                .isDuplicate(event.getEventId()));

        // Give extra time to confirm delivery was NOT called
        TimeUnit.SECONDS.sleep(2);
        verify(deliveryService, never()).deliver(any());
        verify(statusTrackingService, never()).updateStatus(
                any(), eq(NotificationStatus.PROCESSING));
    }

    @Test
    @DisplayName("Should update status to FAILED when delivery throws")
    void shouldUpdateStatusToFailedOnDeliveryException() throws Exception {
        // Given: not a duplicate, but delivery fails
        when(deduplicationService.isDuplicate(any())).thenReturn(false);
        doThrow(new com.courier.common.exception.DeliveryException("SMTP unavailable"))
                .when(deliveryService).deliver(any());

        NotificationEvent event = buildEvent(Channel.SMS);

        // When
        kafkaTemplate.send("courier.sms", event.getCorrelationId(), event).get();

        // Then: status updated to FAILED before rethrowing
        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() ->
                        verify(statusTrackingService, atLeastOnce()).updateStatus(
                                eq(event.getNotificationId()),
                                eq(NotificationStatus.FAILED),
                                any()));
    }

    private NotificationEvent buildEvent(Channel channel) {
        return NotificationEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .notificationId(UUID.randomUUID())
                .correlationId("test-" + UUID.randomUUID())
                .channel(channel)
                .recipient("test@example.com")
                .subject("Test")
                .content("Test content")
                .timestamp(System.currentTimeMillis())
                .sourceService("courier-api")
                .build();
    }
}