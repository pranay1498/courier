package com.courier.common.dto;

import com.courier.common.entity.Notification;
import com.courier.common.enums.Channel;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.enums.Priority;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/*
 * What courier-api returns to the client after accepting a request.
 * The client stores notificationId and uses it to poll /status/{id}.
 *
 * Deliberately does NOT expose: internal retry counts, error messages,
 * raw DB fields. Clients get only what they need.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationResponse {

    private UUID notificationId;
    private String correlationId;
    private Channel channel;
    private NotificationStatus status;
    private Priority priority;
    private Instant createdAt;
    private Instant updatedAt;

    /*
     * Static factory method - converts entity to response DTO.
     * Lives here (not in a separate mapper class) because this
     * conversion is simple and doesn't need a framework.
     * Add MapStruct later if mappings become complex.
     */
    public static NotificationResponse from(Notification notification) {
        return NotificationResponse.builder()
                .notificationId(notification.getId())
                .correlationId(notification.getCorrelationId())
                .channel(notification.getChannel())
                .status(notification.getStatus())
                .priority(notification.getPriority())
                .createdAt(notification.getCreatedAt())
                .updatedAt(notification.getUpdatedAt())
                .build();
    }
}