package com.courier.api.service;

import com.courier.common.enums.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class RateLimiterService {

    private final RedisTemplate<String, String> redisTemplate;
    private final RedisScript<Long> rateLimitScript;
    private final int maxRequests;

    /*
     * Window: 1 minute (60,000 milliseconds).
     * Limit: configurable per authenticated client, per channel, per window
     * (courier.rate-limit.max-requests, default 1,000).
     *
     * Make it configurable rather than a constant so tests can use a tiny
     * limit and deployments can tune it. Adjust per channel in production -
     * SMS limits are often stricter than email due to carrier restrictions.
     */
    private static final long WINDOW_MS = 60_000L;

    public RateLimiterService(
            RedisTemplate<String, String> redisTemplate,
            RedisScript<Long> rateLimitScript,
            @Value("${courier.rate-limit.max-requests:1000}") int maxRequests) {
        this.redisTemplate = redisTemplate;
        this.rateLimitScript = rateLimitScript;
        this.maxRequests = maxRequests;
    }

    /*
     * Key format: "ratelimit:{identifier}:{channel}"
     *
     * identifier is the authenticated clientId (from the API key), so all
     * requests from one client share a bucket. Each channel has its own
     * bucket - exhausting your email limit does not affect your SMS limit.
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
                    String.valueOf(maxRequests) // ARGV[3] - max requests
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
