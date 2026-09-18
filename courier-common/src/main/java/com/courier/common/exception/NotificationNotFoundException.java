package com.courier.common.exception;

import java.util.UUID;

public class NotificationNotFoundException extends RuntimeException {

    public NotificationNotFoundException(UUID id) {
        super("Notification not found: " + id);
    }

    public NotificationNotFoundException(String correlationId) {
        super("Notification not found for correlationId: " + correlationId);
    }
}