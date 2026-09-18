package com.courier.dashboard.service;

import com.courier.common.enums.NotificationStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.time.Instant;

@Service
@RequiredArgsConstructor
@Slf4j
public class StatusUpdateListener {

    private final RedisTemplate<String, String> redisTemplate;

    private static final Duration STATUS_TTL = Duration.ofHours(2);

    /*
     * Called by MessageListenerAdapter for every message on
     * "courier:status-updates".
     *
     * Message format: "{notificationId}:{status}"
     * e.g. "9a1bb794-1c26-438a-85d6-50db99b7ef31:DELIVERED"
     *
     * What we do here:
     * Update the Redis status Hash so the next dashboard read
     * is a cache hit with the correct status - without waiting
     * for the next DB poll or cache expiry.
     *
     * This keeps the dashboard's Redis cache in sync with the
     * worker's state changes in real time.
     */
    public void handleStatusUpdate(String message) {
        try {
            String[] parts = message.split(":");
            if (parts.length != 2) {
                log.warn("Malformed status update message: {}", message);
                return;
            }

            String notificationId = parts[0];
            String status = parts[1];

            // Validate status value
            NotificationStatus.valueOf(status);

            String key = "status:" + notificationId;
            redisTemplate.opsForHash().put(key, "status", status);
            redisTemplate.opsForHash().put(key, "updatedAt",
                    Instant.now().toString());
            redisTemplate.expire(key, STATUS_TTL);

            log.debug("Status cache updated via Pub/Sub. " +
                    "notificationId={} status={}", notificationId, status);

        } catch (IllegalArgumentException e) {
            log.warn("Unknown status value in Pub/Sub message: {}", message);
        } catch (Exception e) {
            log.error("Failed to handle status update. message={} error={}",
                    message, e.getMessage());
        }
    }
}