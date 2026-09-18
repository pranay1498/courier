package com.courier.common.enums;

public enum NotificationStatus {
    PENDING,        // saved to DB, not yet published to Kafka
    QUEUED,         // published to Kafka, not yet picked up by worker
    PROCESSING,     // worker has picked it up, delivery in progress
    DELIVERED,      // successfully delivered to recipient
    FAILED,         // delivery failed, retries exhausted - but before DLQ
    DLQ             // moved to dead letter queue after all retries failed
}
