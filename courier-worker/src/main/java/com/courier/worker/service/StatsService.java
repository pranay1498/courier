package com.courier.worker.service;

import com.courier.common.enums.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class StatsService {

    private final RedisTemplate<String, String> redisTemplate;

    /*
     * Increments the appropriate counters for a delivery outcome.
     *
     * Redis Hash structure:
     *   Key:   "courier:stats:{CHANNEL}"  e.g. "courier:stats:EMAIL"
     *   Fields: total, delivered, failed, dlq, pending
     *
     * HINCRBY is atomic - safe for concurrent worker instances.
     * No TTL on these keys - stats are cumulative and permanent.
     * Reset manually if needed (e.g. after a data migration).
     */
    public void recordDelivered(Channel channel) {
        String key = "courier:stats:" + channel.name();
        redisTemplate.opsForHash().increment(key, "total", 1);
        redisTemplate.opsForHash().increment(key, "delivered", 1);
        log.debug("Stats recorded: delivered. channel={}", channel);
    }

    public void recordFailed(Channel channel) {
        String key = "courier:stats:" + channel.name();
        redisTemplate.opsForHash().increment(key, "total", 1);
        redisTemplate.opsForHash().increment(key, "failed", 1);
        log.debug("Stats recorded: failed. channel={}", channel);
    }

    public void recordDlq(Channel channel) {
        String key = "courier:stats:" + channel.name();
        redisTemplate.opsForHash().increment(key, "dlq", 1);
        log.debug("Stats recorded: dlq. channel={}", channel);
    }
}