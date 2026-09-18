package com.courier.common.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
        name = "delivery_attempts",
        indexes = {
                @Index(name = "idx_attempts_notif_id", columnList = "notification_id")
        }
)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeliveryAttempt {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    /*
     * FetchType.LAZY: do not load the parent Notification
     * when you fetch a DeliveryAttempt. Only load it if
     * you explicitly call attempt.getNotification().
     * EAGER would join the notifications table on every
     * delivery_attempts query - wasteful when you only need
     * the attempt data.
     */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "notification_id", nullable = false)
    private Notification notification;

    @Column(name = "attempt_number", nullable = false)
    private Integer attemptNumber;

    @Column(nullable = false, length = 50)
    private String status;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "attempted_at")
    private Instant attemptedAt;

    @PrePersist
    void prePersist() {
        this.attemptedAt = Instant.now();
    }
}