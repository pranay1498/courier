package com.courier.api.admin;

import com.courier.api.kafka.NotificationProducer;
import com.courier.common.dto.NotificationEvent;
import com.courier.common.entity.Notification;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.exception.NotificationNotFoundException;
import com.courier.common.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReplayService {

    private final NotificationRepository            notificationRepository;
    private final NotificationProducer              notificationProducer;
    private final RedisTemplate<String, String>     redisTemplate;

    @Transactional
    public void replay(UUID notificationId) {

        // ── Step 1: Load and validate ──────────────────────────────────────
        Notification notification = notificationRepository
                .findById(notificationId)
                .orElseThrow(() -> new NotificationNotFoundException(notificationId));

        /*
         * Only DLQ notifications can be replayed.
         * Replaying a DELIVERED notification would cause a duplicate delivery.
         * Replaying a PROCESSING notification would cause a race condition.
         * Fail loudly with a clear message rather than silently proceeding.
         */
        if (notification.getStatus() != NotificationStatus.DLQ) {
            throw new IllegalStateException(
                    "Only DLQ notifications can be replayed. " +
                            "Current status: " + notification.getStatus());
        }

        log.info("Replaying notification. notificationId={} correlationId={}",
                notificationId, notification.getCorrelationId());

        // ── Step 2: Clear the dedup key ────────────────────────────────────
        /*
         * The original eventId's dedup key was set by markProcessed()
         * when the message was first processed - or was never set if all
         * attempts failed before a successful delivery.
         *
         * Either way, we clear it to be safe. Without this, the worker's
         * isDuplicate() check would return true and skip the replayed message.
         *
         * We don't know the original eventId here - it lived on the Kafka
         * message, not in the DB. We'll generate a new eventId below, so
         * the dedup key will be a fresh key anyway. But we also clear the
         * status cache so the dashboard reflects the reset immediately.
         */
        String statusKey = "status:" + notificationId;
        redisTemplate.delete(statusKey);

        log.debug("Cleared Redis status cache. notificationId={}", notificationId);

        // ── Step 3: Reset status to PENDING ───────────────────────────────
        notification.setStatus(NotificationStatus.PENDING);
        notification.setErrorMessage(null);
        notification.setRetryCount(0);
        notificationRepository.save(notification);

        log.info("Reset notification to PENDING. notificationId={}", notificationId);

        // ── Step 4: Build and publish a new event ─────────────────────────
        /*
         * Generate a NEW eventId - critical.
         * If we reused the original eventId and the dedup key still
         * existed somewhere, the worker would skip it.
         * A new eventId guarantees the worker processes this as fresh.
         *
         * correlationId stays the same - this is still the same logical
         * notification. The idempotency check in NotificationService uses
         * correlationId, but replay bypasses that check entirely since
         * we're explicitly re-queuing a known notification.
         */
        NotificationEvent event = NotificationEvent.builder()
                .eventId(UUID.randomUUID().toString())  // fresh eventId
                .notificationId(notification.getId())
                .correlationId(notification.getCorrelationId())
                .channel(notification.getChannel())
                .recipient(notification.getRecipient())
                .subject(notification.getSubject())
                .content(notification.getContent())
                .priority(notification.getPriority())
                .timestamp(System.currentTimeMillis())
                .sourceService("courier-api-replay")    // distinguishes replay events in logs
                .build();

        notificationProducer.publish(event);

        // ── Step 5: Update status to QUEUED ───────────────────────────────
        notification.setStatus(NotificationStatus.QUEUED);
        notificationRepository.save(notification);

        log.info("Notification queued for replay. notificationId={} newEventId={}",
                notificationId, event.getEventId());
    }
}