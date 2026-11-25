package com.courier.api.service;

import com.courier.api.kafka.NotificationProducer;
import com.courier.common.dto.NotificationEvent;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Instant;
import java.util.UUID;

/*
 * The single place where a persisted PENDING notification is handed to Kafka.
 * Used by both the submit path and the admin replay path, so the rules below
 * cannot drift apart.
 *
 * Rules:
 *  1. Publish only AFTER the surrounding DB transaction commits. Publishing
 *     inside it lets the worker consume an event whose row is not visible
 *     yet, and lets the ack callback below match zero uncommitted rows.
 *  2. A row becomes QUEUED only when the broker acknowledges the message -
 *     status reflects what is confirmed, not what was attempted.
 *  3. If the publish fails, the committed row simply stays PENDING and the
 *     reconciler re-publishes it. Never fail the caller for that.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class EventDispatcher {

    private final NotificationProducer     producer;
    private final NotificationRepository   repository;

    /*
     * Publish once the surrounding transaction has committed. With no active
     * transaction (plain unit tests) publish immediately.
     */
    public void dispatchAfterCommit(NotificationEvent event, UUID notificationId) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(
                    new TransactionSynchronization() {
                        @Override
                        public void afterCommit() {
                            publishAndMarkQueued(event, notificationId);
                        }
                    });
        } else {
            publishAndMarkQueued(event, notificationId);
        }
    }

    private void publishAndMarkQueued(NotificationEvent event, UUID id) {
        try {
            producer.publish(event).whenComplete((result, ex) -> {
                if (ex == null) {
                    /*
                     * Compare-and-set: only PENDING -> QUEUED. If the worker
                     * already consumed the message and set PROCESSING, this is
                     * a no-op rather than rolling the status backwards.
                     */
                    repository.transitionStatus(
                            id, NotificationStatus.PENDING, NotificationStatus.QUEUED,
                            Instant.now());
                } else {
                    log.warn("Publish not acknowledged; row stays PENDING for the " +
                            "reconciler. notificationId={} error={}", id, ex.toString());
                }
            });
            log.info("Event handed to producer. notificationId={} eventId={} channel={}",
                    id, event.getEventId(), event.getChannel());
        } catch (Exception e) {
            // The row is committed and PENDING; the reconciler will retry it.
            log.error("Publish failed synchronously; reconciler will retry. notificationId={}",
                    id, e);
        }
    }
}
