package com.courier.api.service;

import com.courier.api.kafka.NotificationProducer;
import com.courier.common.dto.NotificationEvent;
import com.courier.common.dto.NotificationRequest;
import com.courier.common.dto.NotificationResponse;
import com.courier.common.entity.Notification;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.exception.NotificationNotFoundException;
import com.courier.common.exception.RateLimitExceededException;
import com.courier.common.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationService {

    private final NotificationRepository   notificationRepository;
    private final RateLimiterService       rateLimiterService;
    private final NotificationProducer     notificationProducer;

    /*
     * @Transactional wraps the DB operations in a single transaction.
     * If anything throws after the first save() but before the second save(),
     * both saves roll back - the DB stays clean.
     *
     * Important: Kafka publish is NOT transactional.
     * If we publish to Kafka and then the second DB save fails,
     * the Kafka message is already in the broker - it won't roll back.
     * This is an acceptable tradeoff: the worker will process the event,
     * update the status to QUEUED in DB, and everything self-heals.
     *
     * The alternative (Kafka transactions + outbox pattern) adds significant
     * complexity. Not worth it for this system at this scale.
     */
    @Transactional
    public NotificationResponse submit(NotificationRequest request) {

        log.info("Submitting notification. correlationId={} channel={}",
                request.getCorrelationId(), request.getChannel());

        // ── Step 1: Idempotency check ──────────────────────────────────────
        /*
         * Check if we've already processed this correlationId.
         * If yes, return the existing record without any side effects.
         *
         * This handles the case where:
         * - Client sent the request, API processed it, but response was lost
         * - Client's retry logic sends the same request again
         * - Network blip caused a timeout and client is retrying
         *
         * The client gets the same notificationId both times.
         * They can use it to poll /status/{id} and see the current state.
         */
        return notificationRepository
                .findByCorrelationId(request.getCorrelationId())
                .map(existing -> {
                    log.info("Duplicate correlationId detected - returning existing. " +
                                    "correlationId={} notificationId={}",
                            request.getCorrelationId(), existing.getId());
                    return NotificationResponse.from(existing);
                })
                .orElseGet(() -> processNewNotification(request));
    }

    private NotificationResponse processNewNotification(NotificationRequest request) {

        // ── Step 2: Rate limit check ───────────────────────────────────────
        /*
         * Use correlationId as the rate limit identifier.
         * In production you'd extract a userId or tenantId from
         * a JWT token or API key in the request headers.
         *
         * Checked BEFORE saving to DB - no record created for
         * rate-limited requests.
         */
        if (!rateLimiterService.isAllowed(
                request.getCorrelationId(), request.getChannel())) {
            throw new RateLimitExceededException(
                    "Rate limit exceeded for channel: " + request.getChannel() +
                            ". Max 10 requests per minute per channel.");
        }

        // ── Step 3: Persist with status PENDING ───────────────────────────
        /*
         * Save BEFORE publishing to Kafka.
         *
         * If we published first and then the DB save failed,
         * the worker would consume the event, try to find the
         * notification by ID, and find nothing - causing an error
         * that we can't recover from cleanly.
         *
         * By saving first, the record always exists before the
         * worker looks for it, regardless of timing.
         *
         * Status is PENDING - not yet in Kafka, not yet queued.
         */
        Notification notification = Notification.builder()
                .correlationId(request.getCorrelationId())
                .channel(request.getChannel())
                .recipient(request.getRecipient())
                .subject(request.getSubject())
                .content(request.getContent())
                .priority(request.getPriority())
                .status(NotificationStatus.PENDING)
                .build();

        notification = notificationRepository.save(notification);

        log.info("Notification persisted. notificationId={} status=PENDING",
                notification.getId());

        // ── Step 4: Build the internal Kafka event ─────────────────────────
        /*
         * NotificationEvent is different from NotificationRequest.
         * It carries additional fields the worker needs:
         *
         * eventId - a fresh UUID generated HERE, not from the client.
         *   Used by the worker for Redis deduplication (SETNX on eventId).
         *   Different from correlationId (client's key) - this is our key.
         *
         * notificationId - the DB primary key so the worker knows
         *   exactly which row to update.
         *
         * sourceService - for distributed tracing. If you have multiple
         *   services publishing to Kafka, you know which one sent this.
         *
         * timestamp - epoch millis when the event was created.
         *   Useful for detecting stale events in the worker.
         */
        NotificationEvent event = NotificationEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .notificationId(notification.getId())
                .correlationId(request.getCorrelationId())
                .channel(request.getChannel())
                .recipient(request.getRecipient())
                .subject(request.getSubject())
                .content(request.getContent())
                .priority(request.getPriority())
                .metadata(request.getMetadata())
                .timestamp(System.currentTimeMillis())
                .sourceService("courier-api")
                .build();

        // ── Step 5: Publish to Kafka ───────────────────────────────────────
        /*
         * Async send - does not block the current thread.
         * The producer's whenComplete callback handles success/failure logging.
         * If publish fails, the notification stays PENDING in DB.
         * A future background job will re-queue PENDING notifications
         * that are older than a threshold.
         */
        UUID id = notification.getId();
        notificationProducer.publish(event).whenComplete((result, ex) -> {
            if(ex == null){
                /*
                 * Broker acknowledged (acks=all) — now it's genuinely queued.
                 * transitionStatus only moves PENDING → QUEUED. If the worker
                 * already consumed it and set PROCESSING, this is a no-op
                 * rather than rolling the status backwards.
                 */
                notificationRepository.transitionStatus(
                        id, NotificationStatus.PENDING, NotificationStatus.QUEUED, Instant.now());
            }
            // On failure: do nothing. Row stays PENDING, reconciler picks it up.
        });

        log.info("Event published to Kafka. notificationId={} eventId={} channel={}",
                notification.getId(), event.getEventId(), event.getChannel());

        // ── Step 6: Update status to QUEUED ───────────────────────────────
        /*
         * Status moves from PENDING → QUEUED after successful publish.
         * "QUEUED" means: in Kafka, not yet picked up by the worker.
         *
         * Note: if the Kafka publish fails asynchronously (the future
         * completes with an exception), this QUEUED update has already
         * happened. This is the transactional gap mentioned in @Transactional
         * comment above. Acceptable tradeoff.
         */
        notification.setStatus(NotificationStatus.QUEUED);
        notification = notificationRepository.save(notification);

        log.info("Notification status updated to QUEUED. notificationId={}",
                notification.getId());

        return NotificationResponse.from(notification);
    }

    /*
     * Status check - used by GET /api/v1/notifications/{id}/status.
     * Direct DB read for now. In Step 11 (courier-dashboard),
     * this will be Redis-cached.
     */
    @Transactional(readOnly = true)
    public NotificationResponse getStatus(UUID id) {
        return notificationRepository.findById(id)
                .map(NotificationResponse::from)
                .orElseThrow(() -> new NotificationNotFoundException(id));
    }
}