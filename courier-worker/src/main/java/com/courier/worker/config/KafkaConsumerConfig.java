package com.courier.worker.config;

import com.courier.common.dto.NotificationEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, NotificationEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        /*
         * Consumer group ID.
         * All instances of courier-worker share this group ID.
         * Kafka distributes partitions across group members.
         *
         * With 3 partitions and 1 worker instance:
         *   → 1 instance handles all 3 partitions (3 threads)
         * With 3 partitions and 3 worker instances:
         *   → Each instance handles 1 partition (1 thread each)
         * With 3 partitions and 4 worker instances:
         *   → 3 instances handle 1 partition each, 1 sits idle
         *
         * You can never have more active consumers than partitions.
         */
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "courier-worker-group");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                JsonDeserializer.class);

        /*
         * TRUSTED_PACKAGES: the JsonDeserializer will only deserialize
         * classes from these packages. This prevents arbitrary class
         * instantiation from a malicious Kafka message.
         * "*" trusts everything - fine for local dev, restrict in prod.
         */
        props.put(JsonDeserializer.TRUSTED_PACKAGES,
                "com.courier.common.dto");

        /*
         * Tell the deserializer which class to produce.
         * Since we set ADD_TYPE_INFO_HEADERS=false on the producer,
         * there is no __TypeId__ header in the message. We must
         * explicitly tell the consumer what to deserialize into.
         */
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE,
                "com.courier.common.dto.NotificationEvent");

        /*
         * ENABLE_AUTO_COMMIT = false: this is the most critical setting.
         *
         * Auto-commit fires on a timer (default every 5 seconds),
         * committing whatever offset the consumer has reached,
         * regardless of whether your processing code succeeded.
         *
         * If your worker crashes mid-processing, the offset is already
         * committed - the message is permanently lost.
         *
         * With manual commits, we call ack.acknowledge() only AFTER
         * successful delivery. If the worker crashes, the offset was
         * never committed - Kafka redelivers the message on restart.
         */
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        /*
         * AUTO_OFFSET_RESET = earliest:
         * When a consumer group has no committed offset for a partition
         * (first time it connects, or offset was deleted), start from
         * the earliest available message - not the latest.
         *
         * "latest" would skip all messages that arrived before this
         * consumer connected. "earliest" ensures nothing is missed.
         */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /*
         * How many records to fetch from the broker per poll.
         * 10 is conservative - each record triggers a delivery attempt
         * which could involve an HTTP call. Processing 10 records before
         * polling again is a reasonable batch size.
         */
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        /*
         * Maximum time between polls before Kafka considers this
         * consumer dead and triggers a rebalance.
         * 5 minutes is generous - allows for slow delivery handlers.
         * If your webhook delivery takes 30 seconds and you have 10
         * records per poll, you need at least 5 minutes.
         */
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    /*
     * KafkaTemplate for the worker - used only by the DLQ consumer
     * and the DeadLetterPublishingRecoverer to publish failed messages.
     *
     * This is a generic KafkaTemplate<String, Object> because the
     * DLQ can receive any type of message.
     */
    @Bean
    public KafkaTemplate<String, Object> workerKafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.springframework.kafka.support.serializer.JsonSerializer");
        props.put("spring.json.add.type.headers", false);

        org.springframework.kafka.core.DefaultKafkaProducerFactory<String, Object>
                producerFactory = new
                org.springframework.kafka.core.DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NotificationEvent>
    kafkaListenerContainerFactory(
            ConsumerFactory<String, NotificationEvent> consumerFactory,
            KafkaTemplate<String, Object> workerKafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<String, NotificationEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        /*
         * MANUAL_IMMEDIATE: offset is committed the moment
         * ack.acknowledge() is called - immediately, not batched.
         *
         * The alternative MANUAL batches commits and flushes
         * periodically. MANUAL_IMMEDIATE gives you precise control:
         * "commit this specific offset right now."
         */
        factory.getContainerProperties()
                .setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        /*
         * Concurrency = 3 matches our partition count.
         * Spring creates 3 consumer threads, each owning one partition.
         * Messages from partition 0, 1, 2 are processed in parallel.
         *
         * If you set concurrency > partition count, the extra
         * threads sit idle - Kafka won't assign them any partitions.
         */
        factory.setConcurrency(3);

        /*
         * Error handler: what happens when the @KafkaListener method throws.
         *
         * DeadLetterPublishingRecoverer: after all retries are exhausted,
         * publish the failed message to courier.dlq.
         * The lambda (record, ex) -> determines which topic/partition
         * to route to. We always route to courier.dlq partition 0.
         */
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        workerKafkaTemplate,
                        (record, ex) -> {
                            log.error(
                                    "Routing to DLQ. topic={} partition={} offset={} error={}",
                                    record.topic(),
                                    record.partition(),
                                    record.offset(),
                                    ex.getMessage()
                            );
                            return new TopicPartition("courier.dlq", 0);
                        }
                );

        /*
         * FixedBackOff(interval, maxAttempts):
         * Retry up to 3 times with 2-second intervals between attempts.
         * After 3 failures, the recoverer routes the message to DLQ.
         *
         * Total time before DLQ: 2s + 2s + 2s = 6 seconds per message.
         * For transient failures (network blip, temporary service down),
         * 3 retries with 2-second gaps is enough to recover.
         * For permanent failures (invalid recipient, misconfigured handler),
         * retrying is pointless - DLQ is the right destination.
         */
        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3L));

        /*
         * These exceptions should NOT be retried - they indicate a
         * programming error or bad data that retrying won't fix.
         * Route directly to DLQ on first occurrence.
         */
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                IllegalStateException.class
        );

        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }

    @Bean
    public ConsumerFactory<String, Object> dlqConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "courier-dlq-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.courier.common.dto");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE,
                "com.courier.common.dto.NotificationEvent");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        /*
         * latest: on first connect or after offset loss, start from
         * the newest message - don't replay historical DLQ messages.
         * A restarted worker should not re-process DLQ messages that
         * were already handled in a previous session.
         */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object>
    dlqListenerContainerFactory(
            ConsumerFactory<String, Object> dlqConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(dlqConsumerFactory);
        factory.getContainerProperties()
                .setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(1);
        return factory;
    }
}