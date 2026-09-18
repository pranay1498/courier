package com.courier.worker.delivery;

import com.courier.common.dto.NotificationEvent;
import com.courier.common.enums.Channel;
import com.courier.common.exception.DeliveryException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EmailDeliveryHandler implements ChannelDeliveryHandler {

    @Override
    public Channel getChannel() {
        return Channel.EMAIL;
    }

    /*
     * In a real system this would call an email provider API:
     * SendGrid, AWS SES, Mailgun, etc.
     *
     * For this project we simulate delivery with a log statement.
     * The important thing is the surrounding infrastructure:
     * retry logic, DLQ routing, status tracking, deduplication.
     * Those work identically regardless of whether the delivery
     * is a real HTTP call or a log statement.
     *
     * Simulated failure rate: uncomment the block below to test
     * the retry → DLQ path during development. Remove before
     * pushing to GitHub.
     */
    @Override
    public void deliver(NotificationEvent event) throws DeliveryException {
        log.info("EMAIL delivery. eventId={} recipient={} subject={}",
                event.getEventId(),
                event.getRecipient(),
                event.getSubject());


         // Uncomment to test retry → DLQ path:

//         if (Math.random() < 1.0) {
//             throw new DeliveryException(
//                 "Simulated email delivery failure for testing");
//         }

        // Simulate realistic delivery time (remove in production)
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        log.info("EMAIL delivered successfully. eventId={} recipient={}",
                event.getEventId(), event.getRecipient());
    }
}