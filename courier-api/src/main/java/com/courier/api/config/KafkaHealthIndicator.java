package com.courier.api.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Component("kafka")
@RequiredArgsConstructor
@Slf4j
public class KafkaHealthIndicator implements HealthIndicator {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    private static final Set<String> REQUIRED_TOPICS = Set.of(
            "courier.email", "courier.sms",
            "courier.webhook", "courier.dlq"
    );

    /*
     * Checks two things:
     * 1. Can we connect to the Kafka broker at all?
     * 2. Do all four required topics exist?
     *
     * This is more useful than just a ping - if a topic is missing,
     * the producer will fail on first send. Catching it in health
     * check surfaces the problem early.
     */
    @Override
    public Health health() {
        try (AdminClient client = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000"))) {

            Set<String> existingTopics = client
                    .listTopics()
                    .names()
                    .get(5, TimeUnit.SECONDS);

            boolean allTopicsPresent =
                    existingTopics.containsAll(REQUIRED_TOPICS);

            if (allTopicsPresent) {
                return Health.up()
                        .withDetail("broker", bootstrapServers)
                        .withDetail("topics", existingTopics)
                        .build();
            } else {
                Set<String> missing = new java.util.HashSet<>(REQUIRED_TOPICS);
                missing.removeAll(existingTopics);
                return Health.down()
                        .withDetail("broker", bootstrapServers)
                        .withDetail("missingTopics", missing)
                        .build();
            }

        } catch (Exception e) {
            log.error("Kafka health check failed: {}", e.getMessage());
            return Health.down()
                    .withDetail("broker", bootstrapServers)
                    .withException(e)
                    .build();
        }
    }
}