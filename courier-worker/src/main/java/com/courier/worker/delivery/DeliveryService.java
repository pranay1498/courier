package com.courier.worker.delivery;

import com.courier.common.dto.NotificationEvent;
import com.courier.common.enums.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
@Slf4j
public class DeliveryService {

    private final Map<Channel, ChannelDeliveryHandler> handlerMap;

    /*
     * Spring injects ALL beans implementing ChannelDeliveryHandler.
     * Currently: EmailDeliveryHandler, SmsDeliveryHandler, WebhookDeliveryHandler.
     *
     * We convert the list to a Map<Channel, Handler> for O(1) lookup.
     * Adding a new channel handler: annotate the new class with @Component,
     * implement the interface. It appears here automatically on next startup.
     * Zero changes to DeliveryService.
     *
     * Collectors.toMap() throws if two handlers return the same Channel.
     * This is the right behaviour - it catches a programming error
     * (two handlers for the same channel) at startup, not at runtime.
     */
    public DeliveryService(List<ChannelDeliveryHandler> handlers) {
        this.handlerMap = handlers.stream()
                .collect(Collectors.toMap(
                        ChannelDeliveryHandler::getChannel,
                        Function.identity()
                ));
        log.info("Registered delivery handlers for channels: {}",
                handlerMap.keySet());
    }

    /*
     * Route the event to the correct handler.
     * Throws if no handler is registered - this is a programming
     * error (Channel enum has a value with no handler).
     * IllegalStateException is in the not-retryable list in
     * KafkaConsumerConfig, so it goes directly to DLQ.
     */
    public void deliver(NotificationEvent event) {
        ChannelDeliveryHandler handler = handlerMap.get(event.getChannel());

        if (handler == null) {
            throw new IllegalStateException(
                    "No delivery handler registered for channel: "
                            + event.getChannel());
        }

        log.debug("Routing to handler. channel={} eventId={}",
                event.getChannel(), event.getEventId());

        handler.deliver(event);
    }
}