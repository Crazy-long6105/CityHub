package com.hmdp.utils.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.config.KafkaConfig;
import com.hmdp.dto.CacheUpdateMessage;
import com.hmdp.utils.CacheManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class CacheUpdateListener {

    @Resource
    private CacheManager cacheManager;
    @Resource
    private ObjectMapper objectMapper;

    // 监听缓存更新主题
    @KafkaListener(topics = KafkaConfig.CACHE_UPDATE_TOPIC, groupId = "cache_update_group")
    public void listenCacheUpdate(ConsumerRecord<String, CacheUpdateMessage> record, Acknowledgment ack) {
        String key = record.key();
        CacheUpdateMessage message = record.value();

        if (message == null || !CacheManager.supportsLegacyKey(message.getCacheKey())) {
            log.warn("Discarded retired legacy cache event, topic={}, offset={}", record.topic(), record.offset());
            ack.acknowledge();
            return;
        }

        log.info("接收到缓存更新消息，key: {}, message: {}", key, message);

        try {
            if (CacheUpdateMessage.TYPE_UPDATE.equals(message.getType())) {
                // 更新缓存
                cacheManager.updateCache(
                        message.getCacheKey(),
                        message.getCacheValue(),
                        message.getExpireTime(),
                        TimeUnit.SECONDS
                );
                log.info("缓存更新成功，key: {}", message.getCacheKey());
            } else if (CacheUpdateMessage.TYPE_DELETE.equals(message.getType())) {
                // 删除缓存
                cacheManager.deleteCache(message.getCacheKey());
                log.info("缓存删除成功，key: {}", message.getCacheKey());
            }

            // 手动确认消息
            ack.acknowledge();
        } catch (Exception e) {
            throw new IllegalStateException("Legacy cache update failed", e);
        }
    }
}
