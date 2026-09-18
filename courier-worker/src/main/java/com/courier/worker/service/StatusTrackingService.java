package com.courier.worker.service;

import com.courier.common.enums.NotificationStatus;
import com.courier.common.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class StatusTrackingService {

    private final RedisTemplate<String, String> redisTemplate;
    private final NotificationRepository notificationRepository;
    private final NotificationStatusUpdater notificationStatusUpdater;

    /*
     * Status cached for 2 hours.
     * Notifications are typically queried frequently right after
     * submission and rarely after an hour. 2-hour TTL covers
     * the hot read window without wasting Redis memory on stale entries.
     */
    private static final Duration STATUS_TTL = Duration.ofHours(2);

    /*
     * Update status in Redis synchronously (fast, in-memory).
     * Update status in DB asynchronously (@Async - separate thread).
     *
     * Why async DB write?
     * The consumer thread must not be blocked by a DB write.
     * If the DB is slow (under load, network latency), a synchronous
     * write would make the consumer thread fall behind, causing
     * Kafka to think it's dead and trigger a rebalance.
     *
     * Redis is updated synchronously so the status is immediately
     * visible to any status query (courier-dashboard reads Redis first).
     * The DB write catches up asynchronously within milliseconds.
     *
     * Tradeoff: if the async DB write fails, Redis and DB are
     * temporarily inconsistent. The DB status will eventually be
     * corrected - either by the next status update or by a
     * reconciliation job (future improvement).
     */
    public void updateStatus(UUID notificationId,
                             NotificationStatus status,
                             String errorMessage) {

        Instant updatedAt = Instant.now();

        // Synchronous Redis update - immediately visible
        String key = "status:" + notificationId;
        redisTemplate.opsForHash().put(key, "status", status.name());
        redisTemplate.opsForHash().put(key, "updatedAt", updatedAt.toString());
        redisTemplate.expire(key, STATUS_TTL);

        log.debug("Redis status updated. notificationId={} status={}",
                notificationId, status);

        // Async DB update - doesn't block consumer thread
        notificationStatusUpdater.updateStatusInDb(notificationId, status, errorMessage, updatedAt);
    }

    public void updateStatus(UUID notificationId, NotificationStatus status) {
        updateStatus(notificationId, status, null);
    }


    /*
     * Read status - Redis first, DB fallback.
     * Used by courier-dashboard (Step 7).
     */
    public NotificationStatus getStatus(UUID notificationId) {
        String key = "status:" + notificationId;
        String cached = (String) redisTemplate.opsForHash()
                .get(key, "status");

        if (cached != null) {
            log.debug("Status cache HIT. notificationId={}", notificationId);
            return NotificationStatus.valueOf(cached);
        }

        log.debug("Status cache MISS. notificationId={} - querying DB",
                notificationId);

        return notificationRepository.findById(notificationId)
                .map(notification -> {
                    // Repopulate cache for next request
                    redisTemplate.opsForHash().put(
                            key, "status", notification.getStatus().name());
                    redisTemplate.expire(key, STATUS_TTL);
                    return notification.getStatus();
                })
                .orElseThrow(() -> new RuntimeException(
                        "Notification not found: " + notificationId));
    }
}