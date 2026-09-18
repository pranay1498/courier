package com.courier.worker.delivery;

import com.courier.common.dto.NotificationEvent;
import com.courier.common.enums.Channel;
import com.courier.common.exception.DeliveryException;

/*
 * Strategy interface for channel-specific delivery.
 *
 * Each channel (EMAIL, SMS, WEBHOOK) has its own implementation.
 * Adding a new channel = add a new class implementing this interface.
 * Nothing else changes - DeliveryService discovers it automatically
 * via Spring's dependency injection.
 *
 * This is the Open/Closed Principle:
 * Open for extension (new handler class),
 * Closed for modification (existing classes unchanged).
 */
public interface ChannelDeliveryHandler {

    Channel getChannel();

    void deliver(NotificationEvent event) throws DeliveryException;
}