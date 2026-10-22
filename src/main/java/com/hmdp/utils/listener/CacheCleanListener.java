package com.hmdp.utils.listener;

import com.hmdp.config.KafkaConfig;
import com.hmdp.dto.CacheUpdateMessage;
import com.hmdp.utils.CacheManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Slf4j
@Component
public class CacheCleanListener {

    @Resource
    private CacheManager cacheManager;

    // 监听缓存清理主题
    @KafkaListener(topics = KafkaConfig.CACHE_CLEAN_TOPIC, groupId = "cache_clean_group")
    public void listenCacheClean(ConsumerRecord<String, CacheUpdateMessage> record, Acknowledgment ack) {
        String key = record.key();
        CacheUpdateMessage message = record.value();

        if (message == null || !CacheManager.supportsLegacyKey(message.getCacheKey())) {
            log.warn("Discarded retired legacy cache event, topic={}, offset={}", record.topic(), record.offset());
            ack.acknowledge();
            return;
        }

        log.info("接收到缓存清理消息，key: {}", key);

        try {
            // 直接删除缓存（作为最终兜底机制）
            cacheManager.deleteCache(message.getCacheKey());
            log.info("缓存清理成功，key: {}", message.getCacheKey());

            // 手动确认消息
            ack.acknowledge();
        } catch (Exception e) {
            log.error("缓存清理失败，key: {}", key, e);
            throw new IllegalStateException("Legacy cache invalidation failed", e);
        }
    }
}
