package com.courier.api.config;

import com.courier.common.dto.NotificationEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /*
     * ProducerFactory creates and manages Kafka producer instances.
     * A producer is an expensive object - TCP connection to the broker,
     * internal buffers, thread management. DefaultKafkaProducerFactory
     * creates one producer and reuses it across all sends (thread-safe).
     */
    @Bean
    public ProducerFactory<String, NotificationEvent> producerFactory() {
        Map<String, Object> props = new HashMap<>();

        // Where to find the Kafka broker
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        /*
         * Key serializer: correlationId is a String.
         * StringSerializer converts String → UTF-8 bytes.
         */
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        /*
         * Value serializer: NotificationEvent is a Java object.
         * JsonSerializer converts it to JSON bytes using Jackson.
         * The consumer's JsonDeserializer will reverse this.
         */
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        /*
         * acks=all: the send is only considered successful when
         * the partition leader AND all in-sync replicas have written
         * the message to their logs.
         *
         * Trade-off: slower than acks=1 (leader only) but you will
         * never lose a message even if the leader crashes immediately
         * after acknowledging - the replicas already have it.
         *
         * For a notification system, losing a message is worse than
         * a few extra milliseconds of latency. acks=all is correct here.
         */
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        /*
         * Idempotent producer: Kafka assigns this producer a unique
         * Producer ID (PID) and assigns monotonically increasing sequence
         * numbers to each message per partition.
         *
         * If the producer retries (network timeout, broker restart),
         * the broker detects the duplicate sequence number and discards
         * the duplicate. Without this, retries cause duplicate messages.
         *
         * Requires: acks=all and retries > 0.
         */
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        /*
         * With idempotence, up to 5 in-flight requests per connection
         * is safe - the broker tracks sequence numbers and deduplicates.
         * Without idempotence, more than 1 in-flight request could cause
         * reordering on retry.
         */
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        /*
         * Retry up to 3 times on transient failures
         * (broker temporarily unavailable, leader election in progress).
         * With idempotence, retrying is safe - duplicates are filtered.
         *
         * Permanent failures (serialisation error, topic doesn't exist)
         * are NOT retried - they fail immediately.
         */
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        /*
         * Wait 1 second between retries.
         * Without this, three retries fire in milliseconds - no time
         * for a transient issue to resolve itself.
         */
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);

        /*
         * How long to wait for the broker to acknowledge before
         * timing out. 10 seconds is generous - most acks come in
         * under 100ms. If it takes 10 seconds, something is wrong.
         */
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);

        /*
         * ADD_TYPE_INFO_HEADERS = false:
         * By default, Spring's JsonSerializer adds a __TypeId__ header
         * to every Kafka message containing the full Java class name.
         * The consumer uses this to deserialize back to the right class.
         *
         * Problem: if courier-api and courier-worker are in different
         * packages, or if you ever rename the class, the header won't
         * match and deserialization fails.
         *
         * By setting this to false, you remove the type header.
         * The consumer will be configured to always deserialize
         * to NotificationEvent explicitly - more robust.
         */
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        return new DefaultKafkaProducerFactory<>(props);
    }

    /*
     * KafkaTemplate is the primary API for sending messages.
     * It wraps ProducerFactory and provides a clean send() method.
     * Declare it as a Bean so it can be injected anywhere in courier-api.
     */
    @Bean
    public KafkaTemplate<String, NotificationEvent> kafkaTemplate(
            ProducerFactory<String, NotificationEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    /*
     * Topic definitions - created on application startup via KafkaAdmin.
     * Spring detects all NewTopic beans and creates them if they
     * don't already exist. If the topic already exists, it does nothing.
     *
     * partitions=3: allows 3 consumer threads to process in parallel.
     * One thread per partition - adding more threads beyond partition
     * count gives you nothing (extra threads sit idle).
     *
     * replicas=1: fine for local dev with one broker.
     * In production with a 3-broker cluster you'd use replicas=3.
     */
    @Bean
    public org.apache.kafka.clients.admin.NewTopic emailTopic() {
        return TopicBuilder.name("courier.email")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic smsTopic() {
        return TopicBuilder.name("courier.sms")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic webhookTopic() {
        return TopicBuilder.name("courier.webhook")
                .partitions(3)
                .replicas(1)
                .build();
    }

    /*
     * DLQ gets 1 partition - dead letters don't need parallel processing.
     * They're consumed by a single dedicated DLQ consumer for logging
     * and alerting. Volume is low by design (only permanently failed messages).
     */
    @Bean
    public org.apache.kafka.clients.admin.NewTopic dlqTopic() {
        return TopicBuilder.name("courier.dlq")
                .partitions(1)
                .replicas(1)
                .build();
    }
}