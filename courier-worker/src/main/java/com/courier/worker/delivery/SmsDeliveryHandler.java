package com.courier.worker.delivery;

import com.courier.common.dto.NotificationEvent;
import com.courier.common.enums.Channel;
import com.courier.common.exception.DeliveryException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SmsDeliveryHandler implements ChannelDeliveryHandler {

    @Override
    public Channel getChannel() {
        return Channel.SMS;
    }

    @Override
    public void deliver(NotificationEvent event) throws DeliveryException {
        log.info("SMS delivery. eventId={} recipient={}",
                event.getEventId(), event.getRecipient());

        // Real implementation: call Twilio/AWS SNS API here

        log.info("SMS delivered successfully. eventId={} recipient={}",
                event.getEventId(), event.getRecipient());
    }
}