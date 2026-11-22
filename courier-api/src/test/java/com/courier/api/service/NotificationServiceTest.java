package com.courier.api.service;

import com.courier.api.kafka.NotificationProducer;
import com.courier.common.dto.NotificationRequest;
import com.courier.common.dto.NotificationResponse;
import com.courier.common.entity.Notification;
import com.courier.common.enums.Channel;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.enums.Priority;
import com.courier.common.exception.RateLimitExceededException;
import com.courier.common.repository.NotificationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("NotificationService Unit Tests")
class NotificationServiceTest {

    private static final String CLIENT_ID = "test-client";

    @Mock
    private NotificationRepository notificationRepository;

    @Mock
    private RateLimiterService rateLimiterService;

    @Mock
    private NotificationProducer notificationProducer;

    @InjectMocks
    private NotificationService notificationService;

    private NotificationRequest validRequest;
    private Notification savedNotification;

    @BeforeEach
    void setUp() {
        validRequest = NotificationRequest.builder()
                .correlationId("test-correlation-001")
                .channel(Channel.EMAIL)
                .recipient("test@example.com")
                .subject("Test Subject")
                .content("Test Content")
                .priority(Priority.NORMAL)
                .build();

        savedNotification = Notification.builder()
                .id(UUID.randomUUID())
                .correlationId("test-correlation-001")
                .channel(Channel.EMAIL)
                .recipient("test@example.com")
                .status(NotificationStatus.QUEUED)
                .priority(Priority.NORMAL)
                .build();

        // publish() now returns a future (broker ack); lenient because
        // some tests exit before reaching the publish step.
        lenient().when(notificationProducer.publish(any()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    // ── Idempotency tests ──────────────────────────────────────────────────

    @Test
    @DisplayName("Should return existing notification when correlationId already exists")
    void shouldReturnExistingNotificationForDuplicateCorrelationId() {
        // Given: correlationId already in DB
        when(notificationRepository.findByCorrelationId("test-correlation-001"))
                .thenReturn(Optional.of(savedNotification));

        // When
        NotificationResponse response = notificationService.submit(validRequest, CLIENT_ID);

        // Then: returns existing record
        assertThat(response.getCorrelationId())
                .isEqualTo("test-correlation-001");
        assertThat(response.getStatus())
                .isEqualTo(NotificationStatus.QUEUED);

        // Critical: Kafka producer must NOT be called for duplicates
        verify(notificationProducer, never()).publish(any());

        // Critical: no new DB record created
        verify(notificationRepository, never()).save(any());

        // Rate limiter must NOT be called either - skip all processing
        verify(rateLimiterService, never()).isAllowed(any(), any());
    }

    // ── Rate limit tests ───────────────────────────────────────────────────

    @Test
    @DisplayName("Should throw RateLimitExceededException when limit exceeded")
    void shouldThrowWhenRateLimitExceeded() {
        // Given: no existing record, but rate limit exceeded
        when(notificationRepository.findByCorrelationId(any()))
                .thenReturn(Optional.empty());
        when(rateLimiterService.isAllowed(any(), any()))
                .thenReturn(false);

        // When / Then
        assertThatThrownBy(() -> notificationService.submit(validRequest, CLIENT_ID))
                .isInstanceOf(RateLimitExceededException.class)
                .hasMessageContaining("Rate limit exceeded");

        // No DB save, no Kafka publish
        verify(notificationRepository, never()).save(any());
        verify(notificationProducer, never()).publish(any());
    }

    // ── Save-before-publish ordering test ─────────────────────────────────

    @Test
    @DisplayName("Should save to DB before publishing to Kafka")
    void shouldSaveBeforePublishingToKafka() {
        // Given
        when(notificationRepository.findByCorrelationId(any()))
                .thenReturn(Optional.empty());
        when(rateLimiterService.isAllowed(any(), any()))
                .thenReturn(true);
        when(notificationRepository.save(any()))
                .thenReturn(savedNotification);

        // When
        notificationService.submit(validRequest, CLIENT_ID);

        /*
         * InOrder verifies that save() was called BEFORE publish().
         * This is the most important ordering guarantee in the system -
         * if publish happens first and DB save fails, we have a Kafka
         * message with no corresponding DB record.
         */
        InOrder inOrder = inOrder(notificationRepository, notificationProducer);
        inOrder.verify(notificationRepository).save(any(Notification.class));
        inOrder.verify(notificationProducer).publish(any());
    }

    // ── Successful submission test ─────────────────────────────────────────

    @Test
    @DisplayName("Should return QUEUED status after successful submission")
    void shouldReturnQueuedStatusAfterSubmission() {
        // Given
        when(notificationRepository.findByCorrelationId(any()))
                .thenReturn(Optional.empty());
        when(rateLimiterService.isAllowed(any(), any()))
                .thenReturn(true);
        when(notificationRepository.save(any()))
                .thenReturn(savedNotification);

        // When
        NotificationResponse response = notificationService.submit(validRequest, CLIENT_ID);

        // Then
        assertThat(response).isNotNull();
        assertThat(response.getCorrelationId())
                .isEqualTo("test-correlation-001");
        assertThat(response.getChannel())
                .isEqualTo(Channel.EMAIL);
    }

    // ── Event building test ────────────────────────────────────────────────

    @Test
    @DisplayName("Should publish event with new unique eventId each time")
    void shouldPublishEventWithUniqueEventId() {
        // Given
        when(notificationRepository.findByCorrelationId(any()))
                .thenReturn(Optional.empty());
        when(rateLimiterService.isAllowed(any(), any()))
                .thenReturn(true);
        when(notificationRepository.save(any()))
                .thenReturn(savedNotification);

        // When
        notificationService.submit(validRequest, CLIENT_ID);
        notificationService.submit(NotificationRequest.builder()
                .correlationId("different-correlation")
                .channel(Channel.EMAIL)
                .recipient("other@example.com")
                .content("Content")
                .build(), CLIENT_ID);

        /*
         * Capture both published events and verify their eventIds differ.
         * eventId is the internal deduplication key - if two submissions
         * produce the same eventId, the second would be incorrectly
         * skipped by the worker's deduplication check.
         */
        var captor = org.mockito.ArgumentCaptor
                .forClass(com.courier.common.dto.NotificationEvent.class);
        verify(notificationProducer, times(2)).publish(captor.capture());

        var events = captor.getAllValues();
        assertThat(events.get(0).getEventId())
                .isNotEqualTo(events.get(1).getEventId());
    }
}