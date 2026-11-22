package com.courier.api;

import com.courier.api.config.RedisConfig;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.springframework.boot.test.autoconfigure.data.redis.DataRedisTest;

@DataRedisTest
@Import(RedisConfig.class)
public abstract class BaseRedisTest {

    /*
     * Singleton container: started once per JVM, shared by every subclass.
     *
     * Do NOT use @Container on a static field here. Spring caches the
     * application context across test classes, but @Container would stop
     * the container after each class and start a new one on a different
     * port - leaving the cached context pointing at a dead port.
     * Testcontainers' Ryuk sidecar removes the container when the JVM exits.
     */
    static final GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                    .withExposedPorts(6379);

    static {
        redis.start();
    }

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port",
                () -> redis.getMappedPort(6379));
    }

    @Autowired
    protected RedisTemplate<String, String> redisTemplate;

    @BeforeEach
    void cleanRedis() {
        var keys = redisTemplate.keys("*");
        if (keys != null && !keys.isEmpty()) {
            redisTemplate.delete(keys);
        }
    }
}