package com.courier.worker.delivery.sms;

import com.courier.common.exception.DeliveryException;

/*
 * Abstracts the SMS vendor. The handler depends on this interface,
 * not on Twilio/SNS/MessageBird directly.
 *
 * Swapping providers means adding a new implementation and changing
 * which one is active via configuration - no change to the handler.
 */
public interface SmsProvider {

    /**
     * @return the provider's message ID for tracing
     */
    String send(String recipient, String content) throws DeliveryException;

    String getProviderName();
}