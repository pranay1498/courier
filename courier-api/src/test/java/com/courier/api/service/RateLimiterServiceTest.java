package com.courier.api.service;

import com.courier.api.BaseRedisTest;
import com.courier.common.enums.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;



@DisplayName("RateLimiterService Integration Tests")
class RateLimiterServiceTest extends BaseRedisTest {

    private RateLimiterService rateLimiterService;

    @Autowired
    private RedisScript<Long> rateLimitScript;  // ← from RedisConfig, no hardcoding

    @BeforeEach
    void setUp() {
        rateLimiterService = new RateLimiterService(redisTemplate, rateLimitScript);
    }

    @Test
    @DisplayName("Should allow requests under the limit")
    void shouldAllowRequestsUnderLimit() {
        // 10 is the max - first 10 should all be allowed
        for (int i = 0; i < 10; i++) {
            assertThat(rateLimiterService.isAllowed("user-001", Channel.EMAIL))
                    .as("Request %d should be allowed", i + 1)
                    .isTrue();
        }
    }

    @Test
    @DisplayName("Should block the 11th request within the window")
    void shouldBlockRequestsBeyondLimit() {
        // Send 10 allowed requests
        for (int i = 0; i < 10; i++) {
            rateLimiterService.isAllowed("user-002", Channel.EMAIL);
        }

        // 11th should be blocked
        assertThat(rateLimiterService.isAllowed("user-002", Channel.EMAIL))
                .isFalse();
    }

    @Test
    @DisplayName("Should treat each channel independently")
    void shouldTreatChannelsIndependently() {
        // Exhaust EMAIL limit for user-003
        for (int i = 0; i < 10; i++) {
            rateLimiterService.isAllowed("user-003", Channel.EMAIL);
        }

        /*
         * SMS bucket for user-003 should still be open.
         * EMAIL and SMS have separate Redis keys -
         * exhausting one must not affect the other.
         */
        assertThat(rateLimiterService.isAllowed("user-003", Channel.SMS))
                .isTrue();
    }

    @Test
    @DisplayName("Should treat each user independently")
    void shouldTreatUsersIndependently() {
        // Exhaust limit for user-004
        for (int i = 0; i < 10; i++) {
            rateLimiterService.isAllowed("user-004", Channel.EMAIL);
        }

        // user-005 should be unaffected
        assertThat(rateLimiterService.isAllowed("user-005", Channel.EMAIL))
                .isTrue();
    }
}