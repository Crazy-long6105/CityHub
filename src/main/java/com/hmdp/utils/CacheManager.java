package com.hmdp.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.config.KafkaConfig;
import com.hmdp.dto.CacheUpdateMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class CacheManager {

    public static boolean supportsLegacyKey(String key) {
        return key != null && key.matches("voucher:[1-9][0-9]*");
    }

    private static void requireLegacyKey(String key) {
        if (!supportsLegacyKey(key)) throw new IllegalArgumentException("Legacy cache protocol cannot modify this key");
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Resource
    private ObjectMapper objectMapper;

    /**
     * 更新缓存并发送消息到Kafka
     */
    public void updateCacheWithMessage(String key, Object value, Long expireTime, TimeUnit timeUnit) {
        requireLegacyKey(key);
        try {
            // 先更新Redis缓存
            stringRedisTemplate.opsForValue().set(key, objectMapper.writeValueAsString(value), expireTime, timeUnit);

            // 发送缓存更新消息
            CacheUpdateMessage message = new CacheUpdateMessage(
                    CacheUpdateMessage.TYPE_UPDATE, key, value, timeUnit.toSeconds(expireTime), 0
            );
            kafkaTemplate.send(KafkaConfig.CACHE_UPDATE_TOPIC, key, message);
            log.info("缓存更新消息发送成功，key: {}", key);
        } catch (Exception e) {
            log.error("缓存更新失败，key: {}", key, e);
            // 发送缓存清理消息作为兜底
            sendCacheCleanMessage(key);
        }
    }

    /**
     * 删除缓存并发送消息到Kafka
     */
    public void deleteCacheWithMessage(String key) {
        requireLegacyKey(key);
        try {
            // 先删除Redis缓存
            stringRedisTemplate.delete(key);

            // 发送缓存删除消息
            CacheUpdateMessage message = new CacheUpdateMessage(
                    CacheUpdateMessage.TYPE_DELETE, key, null, null, 0
            );
            kafkaTemplate.send(KafkaConfig.CACHE_CLEAN_TOPIC, key, message);
            log.info("缓存删除消息发送成功，key: {}", key);
        } catch (Exception e) {
            log.error("缓存删除失败，key: {}", key, e);
            // 发送缓存清理消息作为兜底
            sendCacheCleanMessage(key);
        }
    }

    /**
     * 发送缓存清理消息（兜底机制）
     */
    public void sendCacheCleanMessage(String key) {
        requireLegacyKey(key);
        try {
            CacheUpdateMessage message = new CacheUpdateMessage(
                    CacheUpdateMessage.TYPE_DELETE, key, null, null, 0
            );
            kafkaTemplate.send(KafkaConfig.CACHE_CLEAN_TOPIC, key, message);
            log.info("缓存清理消息发送成功，key: {}", key);
        } catch (Exception e) {
            log.error("缓存清理消息发送失败，key: {}", key, e);
        }
    }

    /**
     * 直接更新Redis缓存
     */
    public void updateCache(String key, Object value, Long expireTime, TimeUnit timeUnit) {
        requireLegacyKey(key);
        try {
            stringRedisTemplate.opsForValue().set(key, objectMapper.writeValueAsString(value), expireTime, timeUnit);
        } catch (JsonProcessingException e) {
            log.error("缓存序列化失败，key: {}", key, e);
        }
    }

    /**
     * 直接删除Redis缓存
     */
    public void deleteCache(String key) {
        requireLegacyKey(key);
        stringRedisTemplate.delete(key);
    }
}
