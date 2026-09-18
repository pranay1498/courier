--[[
    Sliding Window Rate Limiter

    Why Lua? This algorithm requires 3 Redis operations:
    1. Remove old entries outside the window
    2. Count current entries
    3. Add this request if under the limit

    Between any two of these, another request could arrive
    and get a wrong count. Lua scripts execute atomically in Redis -
    no other command runs between these three operations.
    This is the only correct way to implement a rate limiter in Redis.

    KEYS[1] = the rate limit key, e.g. "ratelimit:user123:EMAIL"
    ARGV[1] = current timestamp in milliseconds
    ARGV[2] = window size in milliseconds (e.g. 60000 for 1 minute)
    ARGV[3] = max requests allowed in the window

    Returns 1 if request is allowed, 0 if rate limit exceeded.
]]

local key         = KEYS[1]
local now         = tonumber(ARGV[1])
local window_ms   = tonumber(ARGV[2])
local max_requests = tonumber(ARGV[3])
local window_start = now - window_ms

-- Step 1: Remove all entries older than the window start
-- ZREMRANGEBYSCORE removes members with score between -inf and window_start
-- This is what makes it a SLIDING window - old entries fall off automatically
redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

-- Step 2: Count how many requests are in the current window
local request_count = redis.call('ZCARD', key)

-- Step 3: If under the limit, add this request and allow it
if request_count < max_requests then
    -- Score = timestamp, Member = timestamp (unique per millisecond)
    -- Using timestamp as both score and member works because
    -- two requests in the same millisecond would collide.
    -- Fix: append a random suffix to make members unique.
    local member = now .. '-' .. math.random(1, 1000000)
    redis.call('ZADD', key, now, member)

    -- Set TTL on the key so Redis auto-cleans it when the window expires
    -- math.ceil converts milliseconds to seconds, rounding up
    redis.call('EXPIRE', key, math.ceil(window_ms / 1000))

    return 1 -- allowed
else
    return 0 -- denied
end