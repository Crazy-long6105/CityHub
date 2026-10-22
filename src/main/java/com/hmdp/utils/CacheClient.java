package com.hmdp.utils;

import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {
    private static final DefaultRedisScript<Long> RELEASE = script("cache_release.lua");
    private static final DefaultRedisScript<Long> PUBLISH = script("cache_publish.lua");
    private static final DefaultRedisScript<Long> FAILURE = script("cache_failure.lua");
    private static final DefaultRedisScript<Long> INVALIDATE = script("cache_invalidate.lua");
    private final StringRedisTemplate redis;
    private final Executor executor;

    public CacheClient(StringRedisTemplate redis, @Qualifier("shopCacheExecutor") Executor executor) {
        this.redis = redis;
        this.executor = executor;
    }

    private static DefaultRedisScript<Long> script(String name) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(name));
        script.setResultType(Long.class);
        return script;
    }

    public static class Unavailable extends RuntimeException {
        public Unavailable(String message) { super(message); }
        public Unavailable(String message, Throwable cause) { super(message, cause); }
    }

    private static class Value<R> {
        final R data;
        final boolean stale;
        Value(R data, boolean stale) { this.data = data; this.stale = stale; }
    }

    private <R> Value<R> read(String key, Class<R> type) {
        String raw = redis.opsForValue().get(key);
        if (raw == null) return null;
        if (raw.isEmpty()) return new Value<>(null, false);
        try {
            RedisData value = JSONUtil.toBean(raw, RedisData.class);
            if (value.getExpireTime() == null || value.getData() == null) {
                throw new IllegalArgumentException("Expected logical expiry envelope");
            }
            R data = JSONUtil.toBean(JSONUtil.toJsonStr(value.getData()), type);
            return new Value<>(data, !value.getExpireTime().isAfter(LocalDateTime.now()));
        } catch (RuntimeException e) {
            log.error("Invalid shop cache envelope, key={}", key, e);
            throw new Unavailable("Cache format unavailable", e);
        }
    }

    public <R, ID> R queryWithLogicalExpire(String prefix, ID id, Class<R> type,
                                          Function<ID, R> fallback, Long time, TimeUnit unit) {
        String key = prefix + id;
        String lock = "lock:shop:" + id;
        String backoff = "cache:shop:backoff:" + id;
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
        try {
            while (true) {
                Value<R> value = read(key, type);
                if (value != null) {
                    if (value.stale) refresh(key, lock, backoff, id, type, fallback, time, unit);
                    return value.data;
                }
                if (Boolean.TRUE.equals(redis.hasKey(backoff))) throw new Unavailable("Shop loading backed off");
                String token = UUID.randomUUID().toString();
                if (Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(lock, token, 10, TimeUnit.SECONDS))) {
                    try {
                        value = read(key, type);
                        if (value != null) return value.data;
                        if (Boolean.TRUE.equals(redis.hasKey(backoff))) throw new Unavailable("Shop loading backed off");
                        R loaded = fallback.apply(id);
                        if (publish(key, lock, backoff, token, loaded, time, unit)) return loaded;
                    } catch (RuntimeException e) {
                        fail(lock, backoff, token);
                        throw e;
                    } finally { release(lock, token); }
                }
                if (System.nanoTime() >= deadline) {
                    log.warn("Shop cold-load wait exhausted, key={}", key);
                    throw new Unavailable("Shop loading busy");
                }
                try { Thread.sleep(50); }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new Unavailable("Shop loading interrupted", e);
                }
            }
        } catch (Unavailable e) { throw e; }
        catch (RuntimeException e) { throw new Unavailable("Shop cache unavailable", e); }
    }

    private <R, ID> void refresh(String key, String lock, String backoff, ID id, Class<R> type,
                                 Function<ID, R> fallback, Long time, TimeUnit unit) {
        String token = UUID.randomUUID().toString();
        try {
            if (Boolean.TRUE.equals(redis.hasKey(backoff)) ||
                    !Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(lock, token, 10, TimeUnit.SECONDS))) return;
            try {
                executor.execute(() -> {
                    try {
                        if (!token.equals(redis.opsForValue().get(lock))) return;
                        Value<R> current = read(key, type);
                        if (current == null || !current.stale) return;
                        publish(key, lock, backoff, token, fallback.apply(id), time, unit);
                    } catch (RuntimeException e) {
                        fail(lock, backoff, token);
                        log.warn("Shop refresh failed, key={}", key, e);
                    } finally { release(lock, token); }
                });
            } catch (RuntimeException e) {
                release(lock, token);
                log.warn("Shop refresh rejected, key={}", key, e);
            }
        } catch (RuntimeException e) {
            // A stale value remains usable when refresh infrastructure fails.
            log.warn("Shop refresh unavailable, key={}", key, e);
        }
    }

    private <R> boolean publish(String key, String lock, String backoff, String token,
                                 R data, Long time, TimeUnit unit) {
        String payload = "";
        if (data != null) {
            RedisData envelope = new RedisData();
            envelope.setData(data);
            envelope.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
            payload = JSONUtil.toJsonStr(envelope);
        }
        boolean published = Long.valueOf(1).equals(redis.execute(PUBLISH, Arrays.asList(key, lock, backoff),
                token, payload, data == null ? "120000" : "0"));
        if (!published) log.warn("Discarded shop rebuild after lease loss, key={}", key);
        return published;
    }

    private void release(String lock, String token) {
        try { redis.execute(RELEASE, Arrays.asList(lock), token); }
        catch (RuntimeException e) { log.warn("Shop lock release unknown; lease will recover, key={}", lock); }
    }

    private void fail(String lock, String backoff, String token) {
        try { redis.execute(FAILURE, Arrays.asList(lock, backoff), token, "5000"); }
        catch (RuntimeException e) { log.warn("Shop refresh backoff unknown, key={}", lock); }
    }

    public void invalidateShop(Long id) {
        Long result = redis.execute(INVALIDATE, Arrays.asList("cache:shop:" + id,
                "lock:shop:" + id, "cache:shop:backoff:" + id));
        if (!Long.valueOf(1).equals(result)) throw new Unavailable("Shop invalidation unconfirmed");
    }
}
