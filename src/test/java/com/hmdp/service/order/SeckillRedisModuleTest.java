package com.hmdp.service.order;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class SeckillRedisModuleTest {
    @Test
    void realRedisBoundsStockAndPreservesQualificationAcrossRetries() throws Exception {
        String host = System.getenv("REDIS_TEST_HOST");
        assumeTrue(host != null && !host.isEmpty(), "Set REDIS_TEST_HOST to run real Redis checks");
        int database = Integer.parseInt(System.getenv().getOrDefault("REDIS_TEST_DATABASE", "15"));
        assertTrue(database > 0 && database <= 15, "Use an isolated Redis test database from 1 to 15");
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(host,
                Integer.parseInt(System.getenv().getOrDefault("REDIS_TEST_PORT", "6379")));
        config.setDatabase(database);
        String password = System.getenv("REDIS_TEST_PASSWORD");
        if (password != null && !password.isEmpty()) config.setPassword(password);
        LettuceClientConfiguration client = LettuceClientConfiguration.builder()
                .commandTimeout(Duration.ofSeconds(2)).shutdownTimeout(Duration.ofMillis(100)).build();
        LettuceConnectionFactory factory = new LettuceConnectionFactory(config, client);
        factory.afterPropertiesSet();
        StringRedisTemplate redis = new StringRedisTemplate(factory);
        SeckillRedisStore store = new SeckillRedisStore(redis);
        long base = ThreadLocalRandom.current().nextLong(1_000_000_000_000L, 8_000_000_000_000L);
        long voucher = base;
        long secondVoucher = base + 1;
        List<String> keys = new ArrayList<>();
        List<String> pendingMembers = new ArrayList<>();
        for (long id : new long[]{voucher, secondVoucher}) {
            keys.add(key("activity", id));
            keys.add(key("stock", id));
            keys.add(key("qualification", id));
        }
        for (int i = 0; i < 45; i++) {
            long order = base + 100 + i;
            keys.add(key("request", order));
            keys.add(key("released", order));
            pendingMembers.add(Long.toString(order));
        }
        ExecutorService workers = Executors.newFixedThreadPool(8);
        boolean ownsNamespace = false;
        try {
            assertEquals(Long.valueOf(0), redis.countExistingKeys(keys), "Random test identity collision");
            for (String member : pendingMembers) {
                assertNull(redis.opsForZSet().score(SeckillRedisStore.PENDING, member), "Random pending identity collision");
            }
            ownsNamespace = true;
            long now = store.read(base + 100).now;
            store.initialize(voucher, 10, now - 60_000, now + 300_000);
            store.initialize(secondVoucher, 5, now - 60_000, now + 300_000);
            List<Callable<Integer>> attempts = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                final int index = i;
                attempts.add(() -> "ACCEPTED".equals(store.reserve(voucher, base + 1000 + index / 2,
                        base + 100 + index, now + 120_000, now + 300_000).get(0)) ? index : -1);
            }
            List<Integer> winners = new ArrayList<>();
            for (Future<Integer> result : workers.invokeAll(attempts, 20, TimeUnit.SECONDS)) {
                int winner = result.get(2, TimeUnit.SECONDS);
                if (winner >= 0) winners.add(winner);
            }
            assertEquals(10, winners.size());
            assertEquals("0", redis.opsForValue().get(key("stock", voucher)));
            assertEquals(Long.valueOf(10), redis.opsForHash().size(key("qualification", voucher)));

            int winner = winners.get(0);
            long user = base + 1000 + winner / 2;
            long oldOrder = base + 100 + winner;
            long replacement = base + 140;
            List<String> repeated = store.reserve(voucher, user, replacement, now + 120_000, now + 300_000);
            assertEquals("EXISTING", repeated.get(0));
            assertEquals(Long.toString(oldOrder), repeated.get(1));
            assertEquals("ACCEPTED", store.reserve(voucher, user, oldOrder, now + 120_000, now + 300_000).get(0));
            assertEquals("0", redis.opsForValue().get(key("stock", voucher)));
            assertFalse(redis.hasKey(key("request", replacement)));

            store.release(oldOrder, user, voucher);
            store.release(oldOrder, user, voucher);
            assertEquals("1", redis.opsForValue().get(key("stock", voucher)));
            assertFalse(redis.opsForHash().hasKey(key("qualification", voucher), Long.toString(user)));
            assertEquals(Long.valueOf(-1), redis.getExpire(key("released", oldOrder)));
            assertEquals("ACCEPTED", store.reserve(voucher, user, replacement, now + 120_000, now + 300_000).get(0));
            store.release(oldOrder, user, voucher);
            // Also exercise the newer-owner branch without relying on the old completion marker.
            redis.delete(key("released", oldOrder));
            store.release(oldOrder, user, voucher);
            assertEquals("0", redis.opsForValue().get(key("stock", voucher)));
            assertEquals(Long.toString(replacement), redis.opsForHash().get(key("qualification", voucher), Long.toString(user)));

            long expiredOrder = base + 141;
            assertEquals("NOT_ACCEPTED", store.reserve(secondVoucher, base + 2000, expiredOrder,
                    now - 1, now + 300_000).get(0));
            assertEquals("5", redis.opsForValue().get(key("stock", secondVoucher)));
            assertFalse(redis.hasKey(key("request", expiredOrder)));
            assertFalse(redis.hasKey(key("qualification", secondVoucher)));
            assertNull(redis.opsForZSet().score(SeckillRedisStore.PENDING, Long.toString(expiredOrder)));

            long invalidOrder = base + 142;
            redis.opsForValue().set(key("qualification", secondVoucher), "wrong-type");
            assertEquals("UNAVAILABLE", store.reserve(secondVoucher, base + 2001, invalidOrder,
                    now + 120_000, now + 300_000).get(0));
            assertEquals("5", redis.opsForValue().get(key("stock", secondVoucher)));
            assertEquals("wrong-type", redis.opsForValue().get(key("qualification", secondVoucher)));
            assertFalse(redis.hasKey(key("request", invalidOrder)));
            assertNull(redis.opsForZSet().score(SeckillRedisStore.PENDING, Long.toString(invalidOrder)));

            redis.opsForList().leftPush(key("released", replacement), "wrong-type");
            assertThrows(RuntimeException.class, () -> store.release(replacement, user, voucher));
            assertEquals("0", redis.opsForValue().get(key("stock", voucher)));
            assertEquals(Long.toString(replacement), redis.opsForHash().get(key("qualification", voucher), Long.toString(user)));
            assertEquals("wrong-type", redis.opsForList().index(key("released", replacement), 0));
        } finally {
            workers.shutdownNow();
            workers.awaitTermination(10, TimeUnit.SECONDS);
            try {
                // Never remove the shared pending index or keys belonging to another request.
                if (ownsNamespace) {
                    redis.opsForZSet().remove(SeckillRedisStore.PENDING, pendingMembers.toArray());
                    redis.delete(keys);
                }
            } finally {
                factory.destroy();
            }
        }
    }

    private static String key(String kind, long id) {
        return SeckillRedisStore.ROOT + kind + ":" + id;
    }
}
