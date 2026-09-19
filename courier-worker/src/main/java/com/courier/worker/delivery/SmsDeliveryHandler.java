package com.courier.worker.delivery;

import com.courier.common.dto.NotificationEvent;
import com.courier.common.enums.Channel;
import com.courier.common.exception.DeliveryException;
import com.courier.worker.delivery.sms.SmsProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class SmsDeliveryHandler implements ChannelDeliveryHandler {

    private final SmsProvider smsProvider;

    @Override
    public Channel getChannel() {
        return Channel.SMS;
    }

    @Override
    public void deliver(NotificationEvent event) throws DeliveryException {
        log.info("SMS delivery. eventId={} recipient={} provider={}",
                event.getEventId(), event.getRecipient(), smsProvider.getProviderName());

        // Real implementation: call Twilio/AWS SNS API here
        String messageId = smsProvider.send(
                event.getRecipient(), event.getContent());

        log.info("SMS delivered successfully. eventId={} recipient={} providerMessageId={}",
                event.getEventId(), event.getRecipient(), messageId);
    }
}