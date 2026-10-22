package com.hmdp.service.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.hmdp.config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class ShopCacheListeners {
    private final ShopCacheOutbox outbox;
    private final ShopDetailCache cache;
    private final JdbcTemplate jdbc;
    private final Cache<String, Boolean> processed = Caffeine.newBuilder().maximumSize(10000)
            .expireAfterWrite(10, TimeUnit.MINUTES).build();

    public ShopCacheListeners(ShopCacheOutbox outbox, ShopDetailCache cache, JdbcTemplate jdbc) {
        this.outbox = outbox;
        this.cache = cache;
        this.jdbc = jdbc;
    }

    @KafkaListener(topics = KafkaConfig.SHOP_CACHE_INVALIDATION_TOPIC, groupId = "shop-cache-work",
            containerFactory = "cacheKafkaListenerContainerFactory")
    public void invalidate(ConsumerRecord<String, String> record, Acknowledgment ack) {
        consume(record, ack, false);
    }

    @KafkaListener(topics = KafkaConfig.SHOP_CACHE_BROADCAST_TOPIC, groupId = "shop-cache-local-${hmdp.instance-id}",
            containerFactory = "cacheKafkaListenerContainerFactory")
    public void broadcast(ConsumerRecord<String, String> record, Acknowledgment ack) {
        consume(record, ack, true);
    }

    private void consume(ConsumerRecord<String, String> record, Acknowledgment ack, boolean broadcast) {
        try {
            ShopCacheOutbox.Event event = outbox.parse(record.value());
            if (broadcast) {
                // Repeated invalidation is safe, even when the short event history has expired.
                cache.invalidate(event.getShopId());
                processed.put(event.getEventId(), Boolean.TRUE);
            } else outbox.execute(event.getEventId(), event.getShopId());
        } catch (ShopCacheOutbox.InvalidEvent e) {
            jdbc.update("INSERT IGNORE INTO tb_cache_message_quarantine"
                            + "(topic,partition_id,offset_id,payload,last_error) VALUES(?,?,?,?,?)",
                    record.topic(), record.partition(), record.offset(), record.value(), e.getMessage());
            log.error("Cache event quarantined, topic={}, offset={}, reason={}", record.topic(), record.offset(), e.getMessage());
        }
        // Database failures intentionally escape so this offset is not acknowledged.
        ack.acknowledge();
    }
}
