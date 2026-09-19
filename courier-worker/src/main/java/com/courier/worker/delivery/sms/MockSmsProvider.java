package com.courier.worker.delivery.sms;

import com.courier.common.exception.DeliveryException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.regex.Pattern;

/*
 * Active by default. Validates the recipient and simulates
 * provider latency, but does not send a real SMS.
 *
 * @ConditionalOnProperty means this bean only loads when
 * courier.sms.provider=mock (or is absent - matchIfMissing).
 * Setting courier.sms.provider=twilio would load a Twilio
 * implementation instead, with zero code changes elsewhere.
 */
@Component
@ConditionalOnProperty(
        name = "courier.sms.provider",
        havingValue = "mock",
        matchIfMissing = true)
@Slf4j
public class MockSmsProvider implements SmsProvider {

    /*
     * E.164 format: + followed by 1-15 digits, first digit non-zero.
     * Real providers reject malformed numbers, so validating here
     * surfaces the same failure without the network round-trip.
     */
    private static final Pattern E164 =
            Pattern.compile("^\\+[1-9]\\d{1,14}$");

    private static final int MAX_SMS_LENGTH = 1600; // concatenated SMS limit

    @Override
    public String send(String recipient, String content)
            throws DeliveryException {

        if (recipient == null || !E164.matcher(recipient).matches()) {
            /*
             * A malformed number will never succeed - retrying is pointless.
             * IllegalArgumentException is in the not-retryable list in
             * KafkaConsumerConfig, so this goes straight to DLQ.
             */
            throw new IllegalArgumentException(
                    "Invalid phone number (expected E.164): " + recipient);
        }

        if (content == null || content.isBlank()) {
            throw new IllegalArgumentException("SMS content must not be blank");
        }

        if (content.length() > MAX_SMS_LENGTH) {
            throw new IllegalArgumentException(
                    "SMS content exceeds " + MAX_SMS_LENGTH
                            + " characters: " + content.length());
        }

        // Simulate provider network latency
        try {
            Thread.sleep(150);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DeliveryException("Interrupted during SMS send", e);
        }

        String messageId = "mock-" + UUID.randomUUID();
        int segments = (int) Math.ceil(content.length() / 160.0);

        log.info("SMS sent via mock provider. recipient={} messageId={} segments={}",
                recipient, messageId, segments);

        return messageId;
    }

    @Override
    public String getProviderName() {
        return "mock";
    }
}