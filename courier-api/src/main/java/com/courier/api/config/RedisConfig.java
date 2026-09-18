package com.courier.api.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    /*
     * RedisTemplate<String, String> is the general-purpose Redis client.
     * Both key and value are Strings - simple and predictable.
     *
     * Why not use the auto-configured RedisTemplate?
     * Spring Boot auto-configures a RedisTemplate<Object, Object>
     * that uses Java serialization for values. Java serialization produces
     * binary output that is unreadable in redis-cli, breaks across JVM
     * versions, and is larger than plain strings.
     *
     * Explicitly configuring StringRedisSerializer on both key and value
     * means all Redis entries are human-readable strings - you can inspect
     * them with redis-cli GET key and see the actual value.
     */
    @Bean
    public RedisTemplate<String, String> redisTemplate(
            RedisConnectionFactory connectionFactory) {

        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);

        StringRedisSerializer serializer = new StringRedisSerializer();
        template.setKeySerializer(serializer);
        template.setValueSerializer(serializer);
        template.setHashKeySerializer(serializer);
        template.setHashValueSerializer(serializer);

        // Must call afterPropertiesSet() when configuring manually
        template.afterPropertiesSet();
        return template;
    }

    /*
     * Register the Lua script as a Spring Bean.
     *
     * When this Bean is created, Spring sends the script to Redis
     * using SCRIPT LOAD, which returns a SHA1 hash.
     * All subsequent calls use EVALSHA (execute by hash) instead
     * of EVAL (send full script each time).
     *
     * Why this matters: EVALSHA is faster and reduces network overhead.
     * More importantly, the script is compiled once by Redis and cached -
     * subsequent executions skip the compilation step.
     *
     * The script file lives at:
     * courier-api/src/main/resources/scripts/rate_limiter.lua
     */
    @Bean
    public RedisScript<Long> rateLimitScript() {
        return RedisScript.of(
                new ClassPathResource("scripts/rate_limiter.lua"),
                Long.class
        );
    }
}