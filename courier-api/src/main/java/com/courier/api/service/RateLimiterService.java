package com.courier.api.service;

import com.courier.common.enums.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class RateLimiterService {

    private final RedisTemplate<String, String> redisTemplate;
    private final RedisScript<Long> rateLimitScript;

    /*
     * Window: 1 minute (60,000 milliseconds)
     * Limit: 10 notifications per channel per minute per user
     *
     * These are deliberately conservative for a notification system.
     * A legitimate use case rarely needs more than 10 emails/min.
     * Adjust per channel in production - SMS limits are often stricter
     * than email due to carrier restrictions.
     */
    private static final long WINDOW_MS    = 60_000L;
    private static final int  MAX_REQUESTS = 10;

    /*
     * Key format: "ratelimit:{identifier}:{channel}"
     *
     * Using correlationId prefix as the identifier here.
     * In a real system you'd use userId or tenantId - a stable
     * identifier that groups all requests from the same client.
     * We're using correlationId's prefix as a proxy for now.
     *
     * Each channel has its own bucket - exhausting your email
     * limit does not affect your SMS limit.
     */
    public boolean isAllowed(String identifier, Channel channel) {
        String key = "ratelimit:" + identifier + ":" + channel.name();
        long now = System.currentTimeMillis();

        try {
            Long result = redisTemplate.execute(
                    rateLimitScript,
                    List.of(key),             // KEYS
                    String.valueOf(now),       // ARGV[1] - current timestamp
                    String.valueOf(WINDOW_MS), // ARGV[2] - window size
                    String.valueOf(MAX_REQUESTS) // ARGV[3] - max requests
            );

            boolean allowed = Long.valueOf(1L).equals(result);

            if (!allowed) {
                log.warn("Rate limit exceeded. key={} channel={}", key, channel);
            }

            return allowed;

        } catch (Exception e) {
            /*
             * Redis is down or the script failed.
             * Fail OPEN - allow the request through.
             *
             * Why fail open and not closed?
             * Rate limiting is a protection mechanism, not a core feature.
             * If Redis is down, blocking all notifications is worse than
             * allowing some extra through temporarily.
             *
             * Log the failure so ops can see Redis is unhealthy.
             * Add an alert here in production.
             */
            log.error("Rate limiter failed - failing open. key={} error={}",
                    key, e.getMessage());
            return true;
        }
    }
}
