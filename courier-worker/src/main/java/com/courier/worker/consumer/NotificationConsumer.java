package com.courier.worker.consumer;

import com.courier.common.dto.NotificationEvent;
import com.courier.common.entity.DeliveryAttempt;
import com.courier.common.entity.Notification;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.repository.DeliveryAttemptRepository;
import com.courier.common.repository.NotificationRepository;
import com.courier.worker.delivery.DeliveryService;
import com.courier.worker.service.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationConsumer {

    private final DeduplicationService   deduplicationService;
    private final StatusTrackingService  statusTrackingService;
    private final DeliveryService        deliveryService;
    private final StatsService statsService;
    private final DeliveryAttemptRepository deliveryAttemptRepository;
    private final NotificationRepository notificationRepository;
    private final StatusPublisher statusPublisher;
    private final CourierMetrics courierMetrics;

    /*
     * @KafkaListener subscribes to all three delivery topics.
     * Spring creates 3 consumer threads (concurrency=3 in config),
     * each assigned to one partition across these topics.
     *
     * groupId matches the one in KafkaConsumerConfig.
     * containerFactory references our custom factory bean.
     *
     * @Payload: the deserialized NotificationEvent.
     * @Header: metadata about where this message came from.
     * Acknowledgment: the manual offset commit handle.
     */
    @KafkaListener(
            topics = {"courier.email", "courier.sms", "courier.webhook"},
            groupId = "courier-worker-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            @Payload NotificationEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {

        MDC.put("eventId", event.getEventId());
        MDC.put("correlationId", event.getCorrelationId());
        MDC.put("channel", event.getChannel().name());
        long startMs = System.currentTimeMillis();
        try {
            log.info("Consumed message. topic={} partition={} offset={}",
                    topic, partition, offset);


            // ── Step 1: Deduplication check ───────────────────────────────
            /*
             * Check if we've already processed this eventId.
             * If yes: this is a Kafka redelivery (consumer crashed before ack).
             * Acknowledge the offset (so Kafka stops redelivering it)
             * but do NOT process again.
             */
            if (deduplicationService.isDuplicate(event.getEventId())) {
                log.info("Duplicate event - acknowledging without processing. " +
                        "eventId={}", event.getEventId());

            }else{
                // ── Step 2: Mark as PROCESSING ───────────────────────────────
                statusTrackingService.updateStatus(
                        event.getNotificationId(),
                        NotificationStatus.PROCESSING);

                // ── Determine attempt number ──────────────────────────────────
                /*
                 * Count existing attempts for this notification - this tells
                 * us which attempt number we're on. First attempt = 1.
                 * This is accurate even across retries because each retry
                 * produces a new call to consume() with the same notificationId.
                 */
                long existingAttempts = deliveryAttemptRepository
                        .countByNotificationId(event.getNotificationId());
                int attemptNumber = (int) existingAttempts + 1;

                // ── Step 3: Deliver ───────────────────────────────────────────
                /*
                 * Routes to the correct handler (EMAIL/SMS/WEBHOOK).
                 * Throws DeliveryException on failure.
                 * The error handler in KafkaConsumerConfig catches the throw,
                 * retries up to 3 times, then routes to DLQ.
                 *
                 * DO NOT catch exceptions here - if you catch and don't rethrow,
                 * the error handler never fires, the offset gets committed,
                 * and the message is silently lost.
                 */
                deliveryService.deliver(event);

                //mark for deduplication
                deduplicationService.markProcessed(event.getEventId());

                // ── Record successful attempt ─────────────────────────────────
                recordAttempt(event, attemptNumber, "DELIVERED", null);

                // ── Step 4: Mark as DELIVERED and commit offset ───────────────
                statusTrackingService.updateStatus(
                        event.getNotificationId(),
                        NotificationStatus.DELIVERED);

                log.info("Message processed successfully. eventId={} notificationId={} attempt={}",
                        event.getEventId(), event.getNotificationId(), attemptNumber);
            }
            /*
             * Commit the offset AFTER successful delivery.
             * This is the manual commit - the guarantee that
             * no message is ever lost.
             *
             * If the process crashes between deliver() and ack(),
             * Kafka redelivers the message. The deduplication check
             * in Step 1 will catch it and skip processing.
             */
            ack.acknowledge();
            statsService.recordDelivered(event.getChannel());
            long durationMs = System.currentTimeMillis() - startMs;
            courierMetrics.recordDelivered(event.getChannel());
            courierMetrics.recordDeliveryDuration(
                    event.getChannel(), "delivered", durationMs);
            statusPublisher.publish(event.getNotificationId(), NotificationStatus.DELIVERED);
        } catch (Exception e) {
            long existingAttempts = deliveryAttemptRepository
                    .countByNotificationId(event.getNotificationId());
            int attemptNumber = (int) existingAttempts + 1;

            // ── Record failed attempt ─────────────────────────────────────
            recordAttempt(event, attemptNumber, "FAILED", e.getMessage());

            /*
             * Delivery failed. Log and rethrow.
             *
             * Rethrowing causes the error handler to fire:
             * - Retry up to 3 times with 2-second backoff
             * - After 3 failures: route to courier.dlq
             * - Update status to DLQ (done in DlqConsumer)
             *
             * Do NOT call ack.acknowledge() here.
             * Not acknowledging means Kafka will redeliver.
             * The retry logic lives in the error handler, not here.
             */
            log.error("Delivery failed. eventId={} attempt={} error={}",
                    event.getEventId(), attemptNumber, e.getMessage());

            // Update status to FAILED before rethrowing
            statusTrackingService.updateStatus(
                    event.getNotificationId(),
                    NotificationStatus.FAILED,
                    e.getMessage());
            statsService.recordFailed(event.getChannel());
            long durationMs = System.currentTimeMillis() - startMs;
            courierMetrics.recordFailed(event.getChannel());
            courierMetrics.recordDeliveryDuration(
                    event.getChannel(), "failed", durationMs);
            statusPublisher.publish(event.getNotificationId(), NotificationStatus.FAILED);
            throw new RuntimeException(
                    "Delivery failed for eventId: " + event.getEventId(), e);
        } finally {
            MDC.clear();
        }
    }

    /*
     * Persists a delivery attempt record.
     * Called on both success and failure paths so the full
     * retry history is visible in the dashboard.
     *
     * Uses notificationRepository to get the Notification entity
     * reference required by the DeliveryAttempt foreign key.
     * findById here is a primary key lookup - always indexed, fast.
     */
    private void recordAttempt(NotificationEvent event,
                               int attemptNumber,
                               String status,
                               String errorMessage) {
        try {
            Notification notification = notificationRepository
                    .findById(event.getNotificationId())
                    .orElse(null);

            if (notification == null) {
                log.warn("Cannot record attempt - notification not found. " +
                        "notificationId={}", event.getNotificationId());
                return;
            }

            DeliveryAttempt attempt = DeliveryAttempt.builder()
                    .notification(notification)
                    .attemptNumber(attemptNumber)
                    .status(status)
                    .errorMessage(errorMessage)
                    .attemptedAt(Instant.now())
                    .build();

            deliveryAttemptRepository.save(attempt);

            log.debug("Delivery attempt recorded. notificationId={} attempt={} status={}",
                    event.getNotificationId(), attemptNumber, status);

        } catch (Exception e) {
            /*
             * Never let attempt recording break delivery processing.
             * Log and continue - losing an audit record is acceptable,
             * losing a delivery is not.
             */
            log.error("Failed to record delivery attempt. notificationId={} error={}",
                    event.getNotificationId(), e.getMessage());
        }
    }
}