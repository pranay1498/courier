package com.courier.dashboard.service;

import com.courier.common.entity.Notification;
import com.courier.common.enums.Channel;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.exception.NotificationNotFoundException;
import com.courier.common.repository.DeliveryAttemptRepository;
import com.courier.common.repository.NotificationRepository;
import com.courier.dashboard.dto.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class DashboardService {

    private final RedisTemplate<String, String>  redisTemplate;
    private final NotificationRepository         notificationRepository;
    private final DeliveryAttemptRepository deliveryAttemptRepository;

    private static final Duration STATUS_TTL = Duration.ofHours(2);

    // ── Status read ────────────────────────────────────────────────────────

    /*
     * Full cache-aside pattern for status reads.
     *
     * Redis stores a Hash per notification:
     *   Key:   "status:{notificationId}"
     *   Field: "status"   Value: "DELIVERED"
     *   Field: "updatedAt" Value: "2026-05-27T..."
     *
     * This Hash is written by courier-worker's StatusTrackingService
     * every time the status changes. By the time a client queries
     * the dashboard, Redis almost always has the current status.
     *
     * DB is the fallback for:
     * - Notifications older than the 2-hour TTL
     * - Notifications that were never picked up by the worker
     * - First query after a Redis restart
     */
    @Transactional(readOnly = true)
    public NotificationStatusResponse getStatus(UUID notificationId) {
        String key = "status:" + notificationId;

        // Check Redis first
        Map<Object, Object> redisEntries =
                redisTemplate.opsForHash().entries(key);

        if (!redisEntries.isEmpty() && redisEntries.containsKey("status")) {
            log.debug("Status cache HIT. notificationId={}", notificationId);

            /*
             * We still need some fields (correlationId, channel, recipient,
             * createdAt) that we don't store in the Redis Hash.
             * Two options:
             *   A) Store the full notification in Redis (more memory, simpler reads)
             *   B) Read basic fields from Redis, fetch metadata from DB
             *
             * We use option B here - status is the hot field, metadata
             * changes rarely. The DB query for metadata is fine because
             * it's a primary key lookup (indexed, fast).
             *
             * If DB latency is a concern at scale, switch to option A
             * and store the full notification JSON in Redis.
             */
            String statusStr = (String) redisEntries.get("status");
            String updatedAt = (String) redisEntries.get("updatedAt");

            return notificationRepository.findById(notificationId)
                    .map(notification -> NotificationStatusResponse.builder()
                            .notificationId(notification.getId())
                            .correlationId(notification.getCorrelationId())
                            .channel(notification.getChannel())
                            .recipient(notification.getRecipient())
                            .status(NotificationStatus.valueOf(statusStr))
                            .priority(notification.getPriority())
                            .errorMessage(notification.getErrorMessage())
                            .createdAt(notification.getCreatedAt())
                            .updatedAt(updatedAt != null
                                    ? Instant.parse(updatedAt)
                                    : notification.getUpdatedAt())
                            .source("REDIS")
                            .build())
                    .orElseThrow(() -> new NoSuchElementException(
                            "Notification not found: " + notificationId));
        }

        // Cache miss - read everything from DB
        log.debug("Status cache MISS. notificationId={} - querying DB",
                notificationId);

        return notificationRepository.findById(notificationId)
                .map(notification -> {
                    // Repopulate Redis for next request
                    repopulateCache(key, notification);

                    return NotificationStatusResponse.builder()
                            .notificationId(notification.getId())
                            .correlationId(notification.getCorrelationId())
                            .channel(notification.getChannel())
                            .recipient(notification.getRecipient())
                            .status(notification.getStatus())
                            .priority(notification.getPriority())
                            .errorMessage(notification.getErrorMessage())
                            .createdAt(notification.getCreatedAt())
                            .updatedAt(notification.getUpdatedAt())
                            .source("DB")
                            .build();
                })
                .orElseThrow(() -> new NoSuchElementException(
                        "Notification not found: " + notificationId));
    }

    private void repopulateCache(String key, Notification notification) {
        redisTemplate.opsForHash().put(
                key, "status", notification.getStatus().name());
        redisTemplate.opsForHash().put(
                key, "updatedAt", notification.getUpdatedAt().toString());
        redisTemplate.expire(key, STATUS_TTL);
    }

    // ── Stats ──────────────────────────────────────────────────────────────

    /*
     * Aggregate delivery statistics per channel.
     *
     * Stats are stored as Redis Hash counters by courier-worker:
     *   Key:   "courier:stats:{CHANNEL}"
     *   Field: "total"     → incremented on every delivery attempt
     *   Field: "delivered" → incremented on successful delivery
     *   Field: "failed"    → incremented on retry-exhausted failure
     *   Field: "dlq"       → incremented when routed to DLQ
     *
     * HINCRBY is atomic - concurrent worker instances incrementing
     * the same counter don't race. No locking needed.
     *
     * These counters don't exist yet - we'll add them to
     * courier-worker in the next sub-step below.
     *
     * Fallback: if Redis counters are empty (Redis just restarted,
     * or worker hasn't incremented yet), fall back to DB COUNT queries.
     * DB counts are slower but always accurate.
     */
    public StatsResponse getStats() {
        Map<String, ChannelStats> byChannel = new LinkedHashMap<>();
        long totalOverall = 0, deliveredOverall = 0,
                failedOverall = 0, dlqOverall = 0, pendingOverall = 0;

        for (Channel channel : Channel.values()) {
            String key = "courier:stats:" + channel.name();
            Map<Object, Object> entries =
                    redisTemplate.opsForHash().entries(key);

            ChannelStats stats;
            if (!entries.isEmpty()) {
                log.debug("Stats cache HIT. channel={}", channel);
                stats = ChannelStats.from(entries);
            } else {
                /*
                 * Redis has no counters for this channel yet.
                 * Fall back to DB COUNT queries.
                 * These are indexed queries - fast enough for
                 * a dashboard endpoint called occasionally.
                 */
                log.debug("Stats cache MISS - querying DB. channel={}", channel);
                stats = getStatsFromDb(channel);
            }

            byChannel.put(channel.name(), stats);

            totalOverall     += stats.getTotal();
            deliveredOverall += stats.getDelivered();
            failedOverall    += stats.getFailed();
            dlqOverall       += stats.getDlq();
            pendingOverall   += stats.getPending();
        }

        ChannelStats overall = ChannelStats.builder()
                .total(totalOverall)
                .delivered(deliveredOverall)
                .failed(failedOverall)
                .dlq(dlqOverall)
                .pending(pendingOverall)
                .build();

        return StatsResponse.builder()
                .byChannel(byChannel)
                .overall(overall)
                .generatedAt(Instant.now())
                .build();
    }

    @Transactional(readOnly = true)
    private ChannelStats getStatsFromDb(Channel channel) {
        return ChannelStats.builder()
                .total(notificationRepository
                        .countByChannelAndStatus(channel, NotificationStatus.DELIVERED)
                        + notificationRepository
                        .countByChannelAndStatus(channel, NotificationStatus.FAILED)
                        + notificationRepository
                        .countByChannelAndStatus(channel, NotificationStatus.DLQ)
                        + notificationRepository
                        .countByChannelAndStatus(channel, NotificationStatus.PENDING)
                        + notificationRepository
                        .countByChannelAndStatus(channel, NotificationStatus.QUEUED)
                        + notificationRepository
                        .countByChannelAndStatus(channel, NotificationStatus.PROCESSING))
                .delivered(notificationRepository
                        .countByChannelAndStatus(channel, NotificationStatus.DELIVERED))
                .failed(notificationRepository
                        .countByChannelAndStatus(channel, NotificationStatus.FAILED))
                .dlq(notificationRepository
                        .countByChannelAndStatus(channel, NotificationStatus.DLQ))
                .pending(notificationRepository
                        .countByChannelAndStatus(channel, NotificationStatus.PENDING)
                        + notificationRepository
                        .countByChannelAndStatus(channel, NotificationStatus.QUEUED))
                .build();
    }

    // ── Listing ────────────────────────────────────────────────────────────

    /*
     * Paginated listing of notifications with optional filters.
     * Always reads from DB - listing is not cached because
     * it's a broad query that changes frequently.
     *
     * Pageable comes from request params automatically:
     * ?page=0&size=20&sort=createdAt,desc
     */
    @Transactional(readOnly = true)
    public Page<NotificationSummary> list(
            NotificationStatus status,
            Channel channel,
            Pageable pageable) {

        Page<Notification> page;

        if (status != null && channel != null) {
            page = notificationRepository
                    .findByChannelAndStatusOrderByCreatedAtDesc(
                            channel, status, pageable);
        } else if (status != null) {
            page = notificationRepository
                    .findByStatusOrderByCreatedAtDesc(status, pageable);
        } else {
            page = notificationRepository.findAll(pageable);
        }

        return page.map(NotificationSummary::from);
    }

    @Transactional(readOnly = true)
    public List<DeliveryAttemptResponse> getDeliveryAttempts(UUID notificationId) {
        /*
         * Verify the notification exists first - return 404
         * rather than an empty list if the ID is wrong.
         */
        if (!notificationRepository.existsById(notificationId)) {
            throw new NotificationNotFoundException(
                    "Notification not found: " + notificationId);
        }

        return deliveryAttemptRepository
                .findByNotificationIdOrderByAttemptNumberAsc(notificationId)
                .stream()
                .map(DeliveryAttemptResponse::from)
                .toList();
    }
}