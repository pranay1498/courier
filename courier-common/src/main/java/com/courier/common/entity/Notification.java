package com.courier.common.entity;

import com.courier.common.enums.Channel;
import com.courier.common.enums.NotificationStatus;
import com.courier.common.enums.Priority;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
        name = "notifications",
        indexes = {
                @Index(name = "idx_notif_correlation", columnList = "correlation_id"),
                @Index(name = "idx_notif_status",      columnList = "status"),
                @Index(name = "idx_notif_channel",     columnList = "channel")
        }
)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Notification {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(updatable = false, nullable = false)
    private UUID id;

    /*
     * correlationId is the client's idempotency key.
     * The UNIQUE constraint here is the last line of defence -
     * even if two API threads race past the application-level check,
     * only one INSERT will succeed. The other gets a constraint violation.
     */
    @Column(name = "correlation_id", unique = true, nullable = false, length = 255)
    private String correlationId;

    /*
     * EnumType.STRING stores "EMAIL" not 0.
     * If you add a new Channel value between EMAIL and SMS,
     * ORDINAL would corrupt every existing row.
     * Always use STRING for enums stored in DB.
     */
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 50)
    private Channel channel;

    @Column(nullable = false, length = 500)
    private String recipient;

    @Column(length = 1000)
    private String subject;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String content;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 50)
    @Builder.Default
    private NotificationStatus status = NotificationStatus.PENDING;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 50)
    @Builder.Default
    private Priority priority = Priority.NORMAL;

    @Column(name = "retry_count")
    @Builder.Default
    private Integer retryCount = 0;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    /*
     * @CreationTimestamp: Hibernate sets this on INSERT automatically.
     * @UpdateTimestamp: Hibernate updates this on every save() call.
     * You never set these manually.
     */
    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private Instant createdAt;

    /*
     * @UpdateTimestamp on the entity only fires when Hibernate manages the entity through save()
     * - it does not fire on JPQL bulk update queries.
     * JPQL updates bypass the Hibernate entity lifecycle entirely. They go straight to SQL.
     */
    @UpdateTimestamp
    @Column(name = "updated_at")
    private Instant updatedAt;

    /*
     * @Version enables optimistic locking.
     * If two worker instances read the same row and both try to update it,
     * the second update will throw OptimisticLockException because
     * the version number has already changed.
     * This prevents lost updates without any database-level locking.
     */
    @Version
    private Long version;
}