package com.courier.worker.service;

import com.courier.worker.BaseRedisTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.redis.DataRedisTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/*
 * @Testcontainers: starts/stops containers automatically per test class.
 * @Container with static field: one container shared across all tests
 * in this class - faster than a new container per test.
 *
 * Why Testcontainers over embedded Redis?
 * Lua scripts (used in rate limiter) have subtle differences between
 * embedded Redis implementations and real Redis. Testcontainers
 * runs the actual redis:7-alpine image - same as production.
 */
@DisplayName("DeduplicationService Integration Tests")
class DeduplicationServiceTest extends BaseRedisTest {

    private DeduplicationService deduplicationService;

    @BeforeEach
    void setUp() {
        deduplicationService = new DeduplicationService(redisTemplate);
    }

    @Test
    @DisplayName("Should return false (not duplicate) for first occurrence of eventId")
    void shouldReturnFalseForFirstOccurrence() {
        boolean isDuplicate = deduplicationService.isDuplicate("event-001");
        assertThat(isDuplicate).isFalse();
    }

    @Test
    @DisplayName("Should return true (is duplicate) for second occurrence of same eventId")
    void shouldReturnTrueForSecondOccurrence() {
        deduplicationService.markProcessed("event-002"); // first call
        boolean isDuplicate = deduplicationService.isDuplicate("event-002"); // second call

        assertThat(isDuplicate).isTrue();
    }

    @Test
    @DisplayName("Should return false again if not yet marked processed")
    void shouldAllowRetryBeforeMarkProcessed() {
        deduplicationService.isDuplicate("event-003"); // check only
        assertThat(deduplicationService.isDuplicate("event-003")).isFalse(); // still false
    }

    @Test
    @DisplayName("Should treat different eventIds as independent")
    void shouldTreatDifferentEventIdsAsIndependent() {
        boolean first  = deduplicationService.isDuplicate("event-003");
        deduplicationService.markProcessed("event-003");
        boolean second = deduplicationService.isDuplicate("event-004");

        assertThat(first).isFalse();
        assertThat(second).isFalse();
    }

    @Test
    @DisplayName("Should set TTL on dedup key")
    void shouldSetTtlOnMarkProcessed() {
        deduplicationService.markProcessed("event-005");

        Long ttl = redisTemplate.getExpire("dedup:event-005");

        /*
         * TTL should be set (positive value).
         * 24 hours = 86400 seconds. Allow some margin for test execution time.
         * If TTL is -1, key exists but has no expiry - memory leak in Redis.
         * If TTL is -2, key doesn't exist - dedup check didn't set it.
         */
        assertThat(ttl)
                .isNotNull()
                .isGreaterThan(86390L)   // close to 24 hours
                .isLessThanOrEqualTo(86400L);
    }
}