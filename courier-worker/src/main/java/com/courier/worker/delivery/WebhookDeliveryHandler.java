package com.courier.worker.delivery;

import com.courier.common.dto.NotificationEvent;
import com.courier.common.enums.Channel;
import com.courier.common.exception.DeliveryException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.*;

import java.time.Duration;
import java.util.Map;

@Component
@Slf4j
public class WebhookDeliveryHandler implements ChannelDeliveryHandler {

    /*
     * RestTemplate is the synchronous HTTP client.
     * We create one here rather than injecting from context
     * because the worker has its own timeout requirements.
     *
     * In production: configure connection and read timeouts,
     * add retry logic specific to webhook calls, handle
     * specific HTTP status codes differently (429 = back off,
     * 410 = remove endpoint, 5xx = retry, 4xx = DLQ immediately).
     */
    private final RestTemplate restTemplate;

    /*
     * Timeouts are the single most important webhook setting.
     * Without them, a hung endpoint holds the consumer thread
     * indefinitely - eventually breaching max.poll.interval.ms
     * and triggering a consumer group rebalance.
     *
     * connectTimeout: how long to wait for the TCP handshake
     * readTimeout: how long to wait for the response after connecting
     */
    public WebhookDeliveryHandler(RestTemplateBuilder builder) {
        this.restTemplate = builder
                .setConnectTimeout(Duration.ofSeconds(5))
                .setReadTimeout(Duration.ofSeconds(10))
                .build();
    }


    @Override
    public Channel getChannel() {
        return Channel.WEBHOOK;
    }

    @Override
    public void deliver(NotificationEvent event) throws DeliveryException {
        log.info("WEBHOOK delivery. eventId={} url={}",
                event.getEventId(), event.getRecipient());

        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            /*
             * X-Courier-Event-Id: allows the recipient to deduplicate
             * on their end. If Courier retries the webhook, the recipient
             * sees the same event ID and knows it's a duplicate.
             */
            headers.set("X-Courier-Event-Id", event.getEventId());

            /*
             * X-Courier-Timestamp: when the event was originally created.
             * Recipient can reject events older than a threshold to
             * prevent replay attacks.
             */
            headers.set("X-Courier-Timestamp",
                    String.valueOf(event.getTimestamp()));

            /*
             * Payload: what we POST to the webhook endpoint.
             * Deliberately minimal - the recipient asked for a notification,
             * we send the notification content. Not our internal IDs.
             */
            Map<String, Object> payload = Map.of(
                    "eventId",    event.getEventId(),
                    "channel",    event.getChannel().name(),
                    "content",    event.getContent(),
                    "metadata",   event.getMetadata() != null
                            ? event.getMetadata()
                            : Map.of(),
                    "timestamp",  event.getTimestamp()
            );

            ResponseEntity<String> response = restTemplate.postForEntity(
                    event.getRecipient(),
                    new HttpEntity<>(payload, headers),
                    String.class
            );

            /*
             * Only 2xx is a success. 3xx, 4xx, 5xx all fail.
             * RestTemplate throws RestClientException for non-2xx by default
             * with the default error handler - but being explicit here makes
             * the intent clear and allows custom handling per status code.
             */
            if (!response.getStatusCode().is2xxSuccessful()) {
                throw new DeliveryException(
                        "Webhook endpoint returned non-2xx status: "
                                + response.getStatusCode()
                                + " for URL: " + event.getRecipient());
            }

            log.info("WEBHOOK delivered successfully. eventId={} url={} status={}",
                    event.getEventId(),
                    event.getRecipient(),
                    response.getStatusCode());

        } catch (HttpClientErrorException e) {
            /*
             * 4xx - the endpoint rejected the request.
             * 429 (rate limited) and 408 (timeout) are worth retrying.
             * Everything else (400 bad request, 404 gone, 401 unauthorized)
             * will fail identically on retry - send straight to DLQ via
             * IllegalArgumentException, which is in the not-retryable list.
             */
            HttpStatusCode status = e.getStatusCode();
            if (status.value() == 429 || status.value() == 408) {
                throw new DeliveryException(
                        "Webhook temporarily unavailable (" + status
                                + ") for " + event.getRecipient(), e);
            }
            throw new IllegalArgumentException(
                    "Webhook permanently rejected the request (" + status
                            + ") for " + event.getRecipient() + " - not retrying", e);

        } catch (HttpServerErrorException e) {
            // 5xx - endpoint is broken but may recover. Retry.
            throw new DeliveryException(
                    "Webhook endpoint error (" + e.getStatusCode()
                            + ") for " + event.getRecipient(), e);

        } catch (ResourceAccessException e) {
            // Connection refused, DNS failure, timeout. Retry.
            throw new DeliveryException(
                    "Webhook unreachable: " + event.getRecipient()
                            + " - " + e.getMessage(), e);
        }
    }
}