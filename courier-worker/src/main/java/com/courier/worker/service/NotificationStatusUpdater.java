package com.courier.worker.service;

import com.courier.common.enums.NotificationStatus;
import com.courier.common.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/*
 * Handles the async DB write for notification status updates.
 * Extracted from StatusTrackingService specifically to avoid the
 * Spring proxy self-invocation problem - @Async and @Transactional
 * only work when the call passes through the Spring proxy,
 * which doesn't happen when a method calls another method
 * on the same class (self-invocation via 'this').
 *
 * By extracting this to a separate @Service, StatusTrackingService
 * calls it as an injected dependency - the call goes through
 * this class's Spring proxy and both annotations fire correctly.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationStatusUpdater {

    private final NotificationRepository notificationRepository;

    @Async("dbUpdateExecutor")
    @Transactional
    public void updateStatusInDb(UUID notificationId,
                                 NotificationStatus status,
                                 String errorMessage, Instant updatedAt) {
        try {
            int updated = notificationRepository.updateStatus(
                    notificationId, status, errorMessage, updatedAt);

            if (updated == 0) {
                log.warn("No notification row updated. " +
                                "notificationId={} status={}",
                        notificationId, status);
            } else {
                log.debug("DB status updated. notificationId={} status={}",
                        notificationId, status);
            }
        } catch (Exception e) {
            log.error("Async DB status update failed. " +
                            "notificationId={} status={} error={}",
                    notificationId, status, e.getMessage());
        }
    }
}