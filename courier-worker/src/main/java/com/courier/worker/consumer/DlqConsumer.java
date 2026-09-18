package com.courier.worker.consumer;

import com.courier.common.enums.Channel;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.repository.NotificationRepository;
import com.courier.worker.service.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class DlqConsumer {

    private final StatusTrackingService statusTrackingService;
    private final StatsService statsService;
    private final NotificationRepository notificationRepository;
    private final DeduplicationService deduplicationService;
    private final StatusPublisher statusPublisher;
    private final CourierMetrics courierMetrics;


    /*
     * Separate consumer group from the main worker.
     * This consumer ONLY processes DLQ messages - it never
     * competes with the main consumer for delivery topic messages.
     *
     * The DLQ consumer's job is simple:
     * 1. Log the permanent failure with full context
     * 2. Update notification status to DLQ in DB
     * 3. (In production) fire an alert to ops team
     * 4. Acknowledge - the message is permanently failed, stop redelivering
     */
    @KafkaListener(
            topics = "courier.dlq",
            groupId = "courier-dlq-group",
            containerFactory = "dlqListenerContainerFactory"
    )
    public void consumeDlq(
            @Payload(required = false) com.courier.common.dto.NotificationEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(value = "kafka_dlt-original-topic",
                    required = false) String originalTopic,
            @Header(value = "kafka_dlt-exception-message",
                    required = false) String exceptionMessage,
            Acknowledgment ack) {

        if (event == null || event.getNotificationId() == null) {
            log.warn("DLQ message has no parseable payload. offset={}", offset);
            ack.acknowledge();
            return;
        }

        /*
         * Spring Kafka's DeadLetterPublishingRecoverer automatically
         * adds headers to the DLQ message:
         *
         * kafka_dlt-original-topic: which topic the message came from
         * kafka_dlt-exception-message: why it failed
         * kafka_dlt-original-partition: which partition
         * kafka_dlt-original-offset: original offset
         *
         * These headers give you full context for debugging
         * without needing to re-read the original topic.
         */
        log.error(
                "PERMANENT FAILURE - Message in DLQ. " +
                        "originalTopic={} offset={} notificationId={} reason={}",
                originalTopic, offset, event.getNotificationId(), exceptionMessage
        );

        // Update status to DLQ in DB if we have the notification ID
        if (event.getNotificationId() != null) {

            /*
             * Idempotency check - if already DLQ, skip all processing.
             * This handles the case where IntelliJ SIGKILL prevents the
             * offset commit from flushing, causing Kafka to redeliver
             * on restart. Processing the same DLQ message twice is safe
             * but noisy - skip cleanly instead.
             */
            try {
                notificationRepository.findById(event.getNotificationId())
                        .ifPresent(notification -> {
                            if (notification.getStatus() == NotificationStatus.DLQ) {
                                log.info("DLQ message already processed - skipping. " +
                                        "notificationId={}", event.getNotificationId());
                                return;
                            }

                            statusTrackingService.updateStatus(
                                    event.getNotificationId(),
                                    NotificationStatus.DLQ,
                                    exceptionMessage);

                            // In production: alertService.sendDlqAlert(originalTopic, exceptionMessage);
                            Channel channel = channelFromTopic(originalTopic);
                            if (channel != null) {
                                statsService.recordDlq(channel);
                            }
                        });
            } catch (Exception e) {
                log.error("Failed to update DLQ status in DB. notificationId={} error={}",
                        event.getNotificationId(), e.getMessage());
            }
        }
        // Inside consumeDlq, after updating status, before ack:
        if (event.getEventId() != null) {
            /*
             * Mark the eventId as processed so the main consumer's
             * dedup check skips it if Kafka redelivers on restart.
             * Without this, a DLQ'd message has no dedup key - the
             * main consumer reprocesses and delivers it on restart.
             */
            deduplicationService.markProcessed(event.getEventId());
            statusPublisher.publish(event.getNotificationId(), NotificationStatus.DLQ);
            courierMetrics.recordDlq(channelFromTopic(originalTopic));
        }
        ack.acknowledge();
    }

    private Channel channelFromTopic(String topic) {
        if (topic == null) return null;
        if (topic.contains("email"))   return Channel.EMAIL;
        if (topic.contains("sms"))     return Channel.SMS;
        if (topic.contains("webhook")) return Channel.WEBHOOK;
        return null;
    }
}