package com.courier.worker.delivery;

import com.courier.common.dto.NotificationEvent;
import com.courier.common.enums.Channel;
import com.courier.common.exception.DeliveryException;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class EmailDeliveryHandler implements ChannelDeliveryHandler {

    private final JavaMailSender mailSender;

    @Value("${courier.mail.from}")
    private String fromAddress;

    @Override
    public Channel getChannel() {
        return Channel.EMAIL;
    }

    /*
     * In a real system this would call an email provider API:
     * SendGrid, AWS SES, Mailgun, etc.
     *
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

            MimeMessage message = mailSender.createMimeMessage();

            /*
             * MimeMessageHelper wraps the raw JavaMail API.
             * multipart=false because we send plain text only.
             * UTF-8 so non-ASCII content (names, emoji) survives.
             */
            MimeMessageHelper helper =
                    new MimeMessageHelper(message, false, "UTF-8");

            helper.setFrom(fromAddress);
            helper.setTo(event.getRecipient());
            helper.setSubject(event.getSubject() != null
                    ? event.getSubject()
                    : "Notification");
            helper.setText(event.getContent());

            /*
             * Custom headers let the recipient's mail system trace
             * the message back to a specific Courier event -
             * same idea as the webhook handler's X-Courier-Event-Id.
             */
            message.addHeader("X-Courier-Event-Id", event.getEventId());
            message.addHeader("X-Courier-Correlation-Id",
                    event.getCorrelationId());

            mailSender.send(message);

            log.info("EMAIL delivered successfully. eventId={} recipient={}",
                    event.getEventId(), event.getRecipient());

        } catch (MailException e) {
            /*
             * MailException covers SMTP connection failures, auth failures,
             * and rejected recipients. All are DeliveryException so the
             * consumer's error handler retries then routes to DLQ.
             */
            throw new DeliveryException(
                    "SMTP delivery failed for " + event.getRecipient()
                            + ": " + e.getMessage(), e);

        } catch (Exception e) {
            // MimeMessageHelper throws MessagingException on malformed input
            throw new DeliveryException(
                    "Failed to build email for " + event.getRecipient()
                            + ": " + e.getMessage(), e);
        }
    }
}