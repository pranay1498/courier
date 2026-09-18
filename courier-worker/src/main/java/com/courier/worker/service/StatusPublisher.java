package com.courier.worker.service;

import com.courier.common.enums.NotificationStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class StatusPublisher {

    private final RedisTemplate<String, String> redisTemplate;

    /*
     * Redis Pub/Sub channel name.
     * All subscribers listening on this channel receive the message.
     * Pattern: "courier:status-updates"
     *
     * Message format: "{notificationId}:{status}"
     * Simple string - no JSON overhead for a lightweight broadcast.
     * The dashboard only needs the ID and new status to update its view.
     */
    private static final String CHANNEL = "courier:status-updates";

    public void publish(UUID notificationId, NotificationStatus status) {
        String message = notificationId + ":" + status.name();

        try {
            redisTemplate.convertAndSend(CHANNEL, message);
            log.debug("Published status update. channel={} notificationId={} status={}",
                    CHANNEL, notificationId, status);
        } catch (Exception e) {
            /*
             * Pub/Sub publish failure must never break delivery processing.
             * The status is already written to Redis Hash and DB by
             * StatusTrackingService - Pub/Sub is best-effort broadcast only.
             */
            log.warn("Failed to publish status update. notificationId={} error={}",
                    notificationId, e.getMessage());
        }
    }
}