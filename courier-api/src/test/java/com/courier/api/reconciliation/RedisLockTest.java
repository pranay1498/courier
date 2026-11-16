package com.courier.api.reconciliation;

import com.courier.api.BaseRedisTest;
import com.courier.api.reconciliation.RedisLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RedisLockTest extends BaseRedisTest {

    private RedisLock lock;

    @BeforeEach
    void setUp() {
        lock = new RedisLock(redisTemplate);
    }

    @Test
    void secondAcquireFailsWhileFirstIsHeld() {
        assertThat(lock.tryAcquire("job", Duration.ofSeconds(30))).isPresent();
        assertThat(lock.tryAcquire("job", Duration.ofSeconds(30))).isEmpty();
    }

    @Test
    void releaseWithWrongTokenDoesNotFreeTheLock() {
        String token = lock.tryAcquire("job", Duration.ofSeconds(30)).orElseThrow();

        lock.release("job", "someone-elses-token");

        // Still held — the ownership check in the Lua script refused the delete
        assertThat(lock.tryAcquire("job", Duration.ofSeconds(30))).isEmpty();

        lock.release("job", token);
        assertThat(lock.tryAcquire("job", Duration.ofSeconds(30))).isPresent();
    }
}