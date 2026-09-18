package com.courier.dashboard.dto;

import com.courier.common.entity.Notification;
import com.courier.common.enums.Channel;
import com.courier.common.enums.NotificationStatus;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/*
 * Lightweight projection for listing endpoints.
 * Does not include content or metadata - reduces
 * payload size when returning hundreds of records.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationSummary {

    private UUID notificationId;
    private String correlationId;
    private Channel channel;
    private String recipient;
    private NotificationStatus status;
    private Instant createdAt;
    private Instant updatedAt;

    public static NotificationSummary from(Notification notification) {
        return NotificationSummary.builder()
                .notificationId(notification.getId())
                .correlationId(notification.getCorrelationId())
                .channel(notification.getChannel())
                .recipient(notification.getRecipient())
                .status(notification.getStatus())
                .createdAt(notification.getCreatedAt())
                .updatedAt(notification.getUpdatedAt())
                .build();
    }
}