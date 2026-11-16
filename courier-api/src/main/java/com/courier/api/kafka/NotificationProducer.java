package com.courier.api.kafka;

import com.courier.common.dto.NotificationEvent;
import com.courier.common.enums.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationProducer {

    private final KafkaTemplate<String, NotificationEvent> kafkaTemplate;

    /*
     * Maps each channel to its topic name.
     * Using Map.of() creates an immutable map - no accidental modification.
     * Adding a new channel: add an entry here + a new topic Bean in config.
     * Nothing else changes.
     */
    private static final Map<Channel, String> TOPIC_MAP = Map.of(
            Channel.EMAIL,   "courier.email",
            Channel.SMS,     "courier.sms",
            Channel.WEBHOOK, "courier.webhook"
    );

    public CompletableFuture<SendResult<String, NotificationEvent>> publish(NotificationEvent event) {
        String topic = TOPIC_MAP.get(event.getChannel());

        if (topic == null) {
            // This should never happen if Channel enum and TOPIC_MAP are in sync.
            // If it does, it's a programming error - throw immediately, don't silently drop.
            throw new IllegalStateException(
                    "No topic configured for channel: " + event.getChannel());
        }

        /*
         * Partition key = correlationId.
         *
         * Kafka hashes the key to determine the partition.
         * All events with the same correlationId go to the same partition.
         * Messages within a partition are strictly ordered.
         *
         * Why correlationId as key?
         * If the same notification generates multiple events (retry scenarios,
         * status updates), they all land in the same partition and are
         * consumed in the order they were produced.
         *
         * If you used a random key or no key, a retry event could be
         * processed before the original event by a different consumer thread.
         */
        CompletableFuture<SendResult<String, NotificationEvent>> future =
                kafkaTemplate.send(topic, event.getCorrelationId(), event);

        /*
         * whenComplete runs after the broker acknowledges (or rejects) the send.
         * This is asynchronous - the calling thread is NOT blocked waiting for
         * the broker response. The API can return 202 to the client while
         * this callback fires in the background.
         *
         * On success: log partition and offset - useful for debugging.
         * On failure: log the error. In a production system you would also:
         *   - Update the notification status to FAILED in DB
         *   - Trigger an alert
         *   - Potentially enqueue a retry via a scheduled job
         */
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                log.error(
                        "Failed to publish event. eventId={} correlationId={} topic={} error={}",
                        event.getEventId(),
                        event.getCorrelationId(),
                        topic,
                        exception.getMessage()
                );
            } else {
                log.info(
                        "Published event. eventId={} correlationId={} topic={} partition={} offset={}",
                        event.getEventId(),
                        event.getCorrelationId(),
                        topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset()
                );
            }
        });
        return future;
    }
}
