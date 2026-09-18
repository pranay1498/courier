package com.courier.dashboard.dto;

import com.courier.common.enums.Channel;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.enums.Priority;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/*
 * Richer than the courier-api NotificationResponse.
 * Includes updatedAt and errorMessage - useful for debugging
 * failed or DLQ'd notifications from the dashboard.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationStatusResponse {

    private UUID notificationId;
    private String correlationId;
    private Channel channel;
    private String recipient;
    private NotificationStatus status;
    private Priority priority;
    private String errorMessage;
    private Instant createdAt;
    private Instant updatedAt;

    /*
     * Indicates whether this response came from Redis cache or DB.
     * Useful for debugging cache behaviour during development.
     * Remove or hide behind a debug flag in production.
     */
    private String source;
}