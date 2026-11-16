package com.courier.api.reconciliation;

import com.courier.api.kafka.NotificationProducer;
import com.courier.common.dto.NotificationEvent;
import com.courier.common.entity.Notification;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.repository.NotificationRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class ReconciliationJob {

    private final NotificationRepository         repository;
    private final NotificationProducer           producer;
    private final RedisTemplate<String, String>  redisTemplate;
    private final RedisLock                      lock;
    private final MeterRegistry                  meterRegistry;

    @Value("${courier.reconciliation.pending-threshold:PT5M}")
    private Duration pendingThreshold;

    @Value("${courier.reconciliation.drift-threshold:PT10M}")
    private Duration driftThreshold;

    private static final Duration LOCK_TTL = Duration.ofSeconds(55);

    private static final Set<NotificationStatus> TERMINAL =
            EnumSet.of(NotificationStatus.DELIVERED, NotificationStatus.DLQ);

    // ── Job 1: re-queue notifications that never reached Kafka ─────────────

    /*
     * fixedDelay (not fixedRate): the next run starts 60s after this one
     * FINISHES. A slow run can never overlap with itself on one instance.
     * The Redis lock prevents overlap across instances.
     */
    @Scheduled(fixedDelayString = "${courier.reconciliation.interval-ms:60000}",
            initialDelayString = "30000")
    public void requeueStuckPending() {
        lock.tryAcquire("reconcile-pending", LOCK_TTL).ifPresentOrElse(token -> {
            try {
                Instant cutoff = Instant.now().minus(pendingThreshold);
                List<Notification> stuck = repository
                        .findTop100ByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
                                NotificationStatus.PENDING, cutoff);

                if (!stuck.isEmpty()) {
                    log.warn("Found {} notifications stuck in PENDING > {}",
                            stuck.size(), pendingThreshold);
                }
                stuck.forEach(this::requeue);
            } finally {
                lock.release("reconcile-pending", token);
            }
        }, () -> log.debug("reconcile-pending held by another instance — skipping"));
    }

    private void requeue(Notification n) {
        NotificationEvent event = NotificationEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .notificationId(n.getId())
                .correlationId(n.getCorrelationId())
                .channel(n.getChannel())
                .recipient(n.getRecipient())
                .subject(n.getSubject())
                .content(n.getContent())
                .priority(n.getPriority())
                .timestamp(System.currentTimeMillis())
                .sourceService("courier-reconciler")
                .build();

        producer.publish(event).whenComplete((r, ex) -> {
            if (ex == null) {
                repository.transitionStatus(n.getId(),
                        NotificationStatus.PENDING, NotificationStatus.QUEUED, Instant.now());
                meterRegistry.counter("courier.reconciliation.requeued",
                        "channel", n.getChannel().name()).increment();
                log.info("Re-queued stuck notification. id={} newEventId={}",
                        n.getId(), event.getEventId());
            }
            // failure: stays PENDING, next cycle retries
        });
    }

    // ── Job 2: repair DB rows that lag behind Redis ────────────────────────

    /*
     * The worker writes Redis synchronously and DB asynchronously. If the
     * async write fails, Redis says DELIVERED while DB still says PROCESSING.
     * Redis is correct here — it was written on the consumer thread after
     * delivery succeeded.
     *
     * Only terminal states are copied Redis → DB. A non-terminal Redis value
     * could be mid-flight; overwriting DB with it would be guessing.
     *
     * driftThreshold (10m) must stay well under the Redis status TTL (2h),
     * otherwise the Redis evidence expires before we look for it.
     */
    @Scheduled(fixedDelayString = "${courier.reconciliation.interval-ms:60000}",
            initialDelayString = "45000")
    public void repairStatusDrift() {
        lock.tryAcquire("reconcile-drift", LOCK_TTL).ifPresent(token -> {
            try {
                Instant cutoff = Instant.now().minus(driftThreshold);
                List<Notification> candidates = repository
                        .findTop100ByStatusInAndUpdatedAtBeforeOrderByUpdatedAtAsc(
                                EnumSet.of(NotificationStatus.PROCESSING,
                                        NotificationStatus.FAILED),
                                cutoff);

                for (Notification n : candidates) {
                    Object cached = redisTemplate.opsForHash()
                            .get("status:" + n.getId(), "status");
                    if (cached == null) continue;

                    NotificationStatus redisStatus = NotificationStatus.valueOf(cached.toString());
                    if (!TERMINAL.contains(redisStatus)) continue;

                    int updated = repository.transitionStatus(
                            n.getId(), n.getStatus(), redisStatus, Instant.now());
                    if (updated == 1) {
                        meterRegistry.counter("courier.reconciliation.repaired",
                                "to", redisStatus.name()).increment();
                        log.warn("Repaired DB drift. id={} db={} → redis={}",
                                n.getId(), n.getStatus(), redisStatus);
                    }
                }
            } finally {
                lock.release("reconcile-drift", token);
            }
        });
    }
}