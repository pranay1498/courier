package com.courier.worker.delivery;

import com.courier.common.dto.NotificationEvent;
import com.courier.common.enums.Channel;
import com.courier.common.exception.DeliveryException;
import com.courier.worker.delivery.DeliveryService;
import com.courier.worker.delivery.EmailDeliveryHandler;
import com.courier.worker.delivery.SmsDeliveryHandler;
import com.courier.worker.delivery.WebhookDeliveryHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("DeliveryService Unit Tests")
class DeliveryServiceTest {

    @Mock
    private EmailDeliveryHandler emailHandler;

    @Mock
    private SmsDeliveryHandler smsHandler;

    @Mock
    private WebhookDeliveryHandler webhookHandler;

    private DeliveryService deliveryService;

    @BeforeEach
    void setUp() {
        // Mock each handler to return its channel
        when(emailHandler.getChannel()).thenReturn(Channel.EMAIL);
        when(smsHandler.getChannel()).thenReturn(Channel.SMS);
        when(webhookHandler.getChannel()).thenReturn(Channel.WEBHOOK);

        deliveryService = new DeliveryService(
                List.of(emailHandler, smsHandler, webhookHandler));
    }

    @Test
    @DisplayName("Should route EMAIL event to EmailDeliveryHandler")
    void shouldRouteEmailToEmailHandler() throws DeliveryException {
        NotificationEvent event = buildEvent(Channel.EMAIL);

        deliveryService.deliver(event);

        verify(emailHandler).deliver(event);
        verify(smsHandler, never()).deliver(any());
        verify(webhookHandler, never()).deliver(any());
    }

    @Test
    @DisplayName("Should route SMS event to SmsDeliveryHandler")
    void shouldRouteSmsToSmsHandler() throws DeliveryException {
        NotificationEvent event = buildEvent(Channel.SMS);

        deliveryService.deliver(event);

        verify(smsHandler).deliver(event);
        verify(emailHandler, never()).deliver(any());
        verify(webhookHandler, never()).deliver(any());
    }

    @Test
    @DisplayName("Should route WEBHOOK event to WebhookDeliveryHandler")
    void shouldRouteWebhookToWebhookHandler() throws DeliveryException {
        NotificationEvent event = buildEvent(Channel.WEBHOOK);

        deliveryService.deliver(event);

        verify(webhookHandler).deliver(event);
        verify(emailHandler, never()).deliver(any());
        verify(smsHandler, never()).deliver(any());
    }

    @Test
    @DisplayName("Should throw IllegalStateException for unregistered channel")
    void shouldThrowForUnregisteredChannel() {
        /*
         * DeliveryService with only email handler registered.
         * Sending an SMS event should throw IllegalStateException.
         * This is in the not-retryable list in KafkaConsumerConfig -
         * goes directly to DLQ without retrying.
         */
        DeliveryService limitedService =
                new DeliveryService(List.of(emailHandler));

        NotificationEvent smsEvent = buildEvent(Channel.SMS);

        assertThatThrownBy(() -> limitedService.deliver(smsEvent))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No delivery handler registered");
    }

    @Test
    @DisplayName("Should propagate DeliveryException from handler")
    void shouldPropagateDeliveryExceptionFromHandler() throws DeliveryException {
        NotificationEvent event = buildEvent(Channel.EMAIL);

        doThrow(new DeliveryException("SMTP server unavailable"))
                .when(emailHandler).deliver(event);

        assertThatThrownBy(() -> deliveryService.deliver(event))
                .isInstanceOf(DeliveryException.class)
                .hasMessage("SMTP server unavailable");
    }

    private NotificationEvent buildEvent(Channel channel) {
        return NotificationEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .notificationId(UUID.randomUUID())
                .correlationId("test-001")
                .channel(channel)
                .recipient("test@example.com")
                .content("Test content")
                .timestamp(System.currentTimeMillis())
                .build();
    }
}