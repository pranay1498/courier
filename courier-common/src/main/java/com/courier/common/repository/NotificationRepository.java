package com.courier.common.repository;

import com.courier.common.entity.Notification;
import com.courier.common.enums.Channel;
import com.courier.common.enums.NotificationStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface NotificationRepository extends JpaRepository<Notification, UUID> {

    /*
     * Used by courier-api for idempotency check.
     * If this returns a value, the correlationId was already processed.
     * Returns the same notificationId to the client without re-processing.
     */
    Optional<Notification> findByCorrelationId(String correlationId);

    /*
     * Used by courier-dashboard to list notifications by status.
     * Returns a Page - supports pagination automatically.
     * SQL generated: SELECT * FROM notifications WHERE status = ?
     *                ORDER BY created_at DESC
     *                LIMIT ? OFFSET ?
     */
    Page<Notification> findByStatusOrderByCreatedAtDesc(
            NotificationStatus status, Pageable pageable);

    /*
     * Used by courier-dashboard for filtered listing.
     */
    Page<Notification> findByChannelAndStatusOrderByCreatedAtDesc(
            Channel channel, NotificationStatus status, Pageable pageable);

    /*
     * Used by courier-dashboard for aggregate stats.
     * COUNT query - does not load full entity objects.
     */
    Long countByStatus(NotificationStatus status);

    Long countByChannelAndStatus(Channel channel, NotificationStatus status);

    /*
     * Used by a future background job to detect stuck notifications.
     * Finds PENDING notifications older than N minutes that never
     * made it to Kafka - allows re-queuing them.
     */
    List<Notification> findByStatusAndCreatedAtBefore(
            NotificationStatus status, Instant cutoff);

    /*
     * Direct status update without loading the full entity.
     * Used by courier-worker for high-frequency status updates
     * (PROCESSING, DELIVERED, FAILED) where loading the full
     * entity first would be wasteful.
     *
     * @Modifying: required for UPDATE/DELETE JPQL queries.
     * Without it, Spring Data throws an exception.
     */
    @Modifying
    @Query("UPDATE Notification n SET n.status = :status, " +
            "n.errorMessage = :errorMessage, " +
            "n.updatedAt = :updatedAt " +
            "WHERE n.id = :id")
    int updateStatus(
            @Param("id") UUID id,
            @Param("status") NotificationStatus status,
            @Param("errorMessage") String errorMessage,
            @Param("updatedAt") Instant updatedAt);
}