package com.hmdp.service.cache;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.time.LocalDateTime;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class LogicalCacheModuleTest {
    @Test
    @SuppressWarnings("unchecked")
    void logicalExpiryColdLoadAndFailureSemantics() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        Function<Long, Shop> database = mock(Function.class);
        CacheClient client = new CacheClient(redis, task -> { throw new RejectedExecutionException(); });

        RedisData envelope = new RedisData();
        envelope.setData(new Shop().setId(1L).setName("old"));
        envelope.setExpireTime(LocalDateTime.now().plusMinutes(30));
        when(values.get("cache:shop:1")).thenReturn(JSONUtil.toJsonStr(envelope));
        assertEquals("old", query(client, database).getName());
        verifyNoInteractions(database);

        envelope.setExpireTime(LocalDateTime.now().minusSeconds(1));
        when(values.get("cache:shop:1")).thenReturn(JSONUtil.toJsonStr(envelope));
        when(values.setIfAbsent(eq("lock:shop:1"), anyString(), eq(10L), eq(TimeUnit.SECONDS))).thenReturn(true);
        assertEquals("old", query(client, database).getName());
        verifyNoInteractions(database);

        when(values.get("cache:shop:1")).thenReturn("");
        assertNull(query(client, database));
        verifyNoInteractions(database);

        when(values.get("cache:shop:1")).thenReturn((String) null);
        when(database.apply(1L)).thenReturn(new Shop().setId(1L).setName("loaded"));
        when(redis.execute(any(RedisScript.class), anyList(), any(), any(), any())).thenReturn(1L);
        assertEquals("loaded", query(client, database).getName());
        verify(database, times(1)).apply(1L);

        when(database.apply(1L)).thenThrow(new IllegalStateException("database offline"));
        assertThrows(CacheClient.Unavailable.class, () -> query(client, database));
        when(values.get("cache:shop:1")).thenThrow(new IllegalStateException("redis offline"));
        assertThrows(CacheClient.Unavailable.class, () -> query(client, database));
        verify(database, times(2)).apply(1L);
    }

    private Shop query(CacheClient cache, Function<Long, Shop> database) {
        return cache.queryWithLogicalExpire("cache:shop:", 1L, Shop.class, database, 30L, TimeUnit.MINUTES);
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "CACHE_REDIS_INTEGRATION", matches = "true")
    void realRedisRejectsRevokedTokensAndPreservesTtlPolicy() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(
                System.getenv().getOrDefault("SPRING_REDIS_HOST", "127.0.0.1"),
                Integer.parseInt(System.getenv().getOrDefault("SPRING_REDIS_PORT", "6379")));
        String password = System.getenv("SPRING_REDIS_PASSWORD");
        if (password != null && !password.isEmpty()) config.setPassword(password);
        LettuceConnectionFactory factory = new LettuceConnectionFactory(config);
        factory.afterPropertiesSet();
        StringRedisTemplate redis = new StringRedisTemplate(factory);
        String namespace = "test:shop-cache:" + UUID.randomUUID();
        java.util.List<String> keys = Arrays.asList(namespace + ":data", namespace + ":lock", namespace + ":backoff");
        DefaultRedisScript<Long> publish = script("cache_publish.lua");
        DefaultRedisScript<Long> invalidate = script("cache_invalidate.lua");
        DefaultRedisScript<Long> release = script("cache_release.lua");
        try {
            redis.opsForValue().set(keys.get(1), "A", 10, TimeUnit.SECONDS);
            assertEquals(1L, redis.execute(publish, keys, "A", "data", "0"));
            assertEquals(-1L, redis.getExpire(keys.get(0), TimeUnit.MILLISECONDS));

            redis.opsForValue().set(keys.get(1), "B", 10, TimeUnit.SECONDS);
            assertEquals(0L, redis.execute(publish, keys, "A", "obsolete", "0"));
            assertEquals(0L, redis.execute(release, Arrays.asList(keys.get(1)), "A"));
            assertEquals("B", redis.opsForValue().get(keys.get(1)));
            assertEquals(1L, redis.execute(publish, keys, "B", "", "120000"));
            Long ttl = redis.getExpire(keys.get(0), TimeUnit.MILLISECONDS);
            assertNotNull(ttl);
            assertTrue(ttl > 115000 && ttl <= 120000);

            redis.opsForValue().set(keys.get(1), "C", 10, TimeUnit.SECONDS);
            assertEquals(1L, redis.execute(invalidate, keys));
            assertEquals(0L, redis.execute(publish, keys, "C", "obsolete", "0"));
            assertNull(redis.opsForValue().get(keys.get(0)));
        } finally {
            redis.delete(keys);
            factory.destroy();
        }
    }

    private DefaultRedisScript<Long> script(String name) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(name));
        script.setResultType(Long.class);
        return script;
    }
}
