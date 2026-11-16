package com.courier.api.reconciliation;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class RedisLock {

    private final RedisTemplate<String, String> redisTemplate;

    /*
     * Release only if we still own the lock. Without the ownership check,
     * this sequence deletes someone else's lock:
     *   1. A acquires lock (TTL 60s)
     *   2. A pauses for 70s (GC, slow DB) — lock expires
     *   3. B acquires lock
     *   4. A resumes, calls DEL — deletes B's lock
     * GET + DEL must be atomic, hence Lua — same reason as the rate limiter.
     */
    private static final DefaultRedisScript<Long> RELEASE = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then " +
                    "  return redis.call('DEL', KEYS[1]) " +
                    "else return 0 end",
            Long.class);

    /** Returns an owner token if acquired, empty if another instance holds it. */
    public Optional<String> tryAcquire(String name, Duration ttl) {
        String token = UUID.randomUUID().toString();
        Boolean ok = redisTemplate.opsForValue()
                .setIfAbsent("lock:" + name, token, ttl);   // SET NX PX
        return Boolean.TRUE.equals(ok) ? Optional.of(token) : Optional.empty();
    }

    public void release(String name, String token) {
        redisTemplate.execute(RELEASE, List.of("lock:" + name), token);
    }
}