package com.hmdp.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import java.util.concurrent.TimeUnit;
import java.util.*;

@Configuration
public class KafkaConfig {
    public static final String SECKILL_ORDER_TOPIC = "seckill_order_topic";
    public static final String CACHE_UPDATE_TOPIC = "cache_update_topic";
    public static final String CACHE_CLEAN_TOPIC = "cache_clean_topic";
    public static final String SHOP_CACHE_INVALIDATION_TOPIC = "shop_cache_invalidation_topic";
    public static final String SHOP_CACHE_BROADCAST_TOPIC = "shop_cache_broadcast_topic";

    @Bean
    public NewTopic shopCacheInvalidationTopic() {
        return TopicBuilder.name(SHOP_CACHE_INVALIDATION_TOPIC).partitions(3).replicas(1)
                .config("retention.ms", "86400000").build();
    }

    @Bean
    public NewTopic shopCacheBroadcastTopic() {
        return TopicBuilder.name(SHOP_CACHE_BROADCAST_TOPIC).partitions(3).replicas(1)
                .config("retention.ms", "86400000").build();
    }

    @Bean
    public NewTopic seckillDeadLetterTopic() {
        return TopicBuilder.name(SECKILL_ORDER_TOPIC + ".DLT").partitions(6).replicas(1)
                .config("retention.ms", "604800000").build();
    }

    @Bean
    public NewTopic legacyCacheUpdateDeadLetterTopic() {
        return TopicBuilder.name(CACHE_UPDATE_TOPIC + ".DLT").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic legacyCacheCleanDeadLetterTopic() {
        return TopicBuilder.name(CACHE_CLEAN_TOPIC + ".DLT").partitions(2).replicas(1).build();
    }

    @Bean(name = "kafkaTemplate")
    public KafkaTemplate<String, Object> kafkaTemplate(KafkaProperties properties) {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(properties.buildProducerProperties()));
    }

    @Bean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            KafkaProperties properties, @Qualifier("kafkaTemplate") KafkaTemplate<String, Object> template) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(properties.buildConsumerProperties()));
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        SeekToCurrentErrorHandler handler = new SeekToCurrentErrorHandler((record, error) -> {
            try {
                Map<String, Object> isolated = new LinkedHashMap<>();
                isolated.put("topic", record.topic());
                isolated.put("partition", record.partition());
                isolated.put("offset", record.offset());
                isolated.put("value", record.value());
                isolated.put("error", error.getClass().getName());
                List<Map<String, String>> headers = new ArrayList<>();
                for (org.apache.kafka.common.header.Header header : record.headers()) {
                    Map<String, String> encoded = new LinkedHashMap<>();
                    encoded.put("key", header.key());
                    encoded.put("value", header.value() == null ? null : Base64.getEncoder().encodeToString(header.value()));
                    headers.add(encoded);
                }
                // ErrorHandlingDeserializer's header preserves the original rejected bytes.
                isolated.put("headers", headers);
                template.send(record.topic() + ".DLT", record.partition(), record.key() == null ? null : record.key().toString(), isolated)
                        .get(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Dead letter confirmation interrupted", e);
            } catch (Exception e) { throw new IllegalStateException("Dead letter not confirmed", e); }
        }, new FixedBackOff(1000, 2));
        handler.setCommitRecovered(true);
        factory.setErrorHandler(handler);
        return factory;
    }

    // 创建Kafka主题
    @Bean
    public NewTopic seckillOrderTopic(){
        return TopicBuilder.name(SECKILL_ORDER_TOPIC)
                .partitions(6)  // 增加分区数提高并发
                .replicas(1)
                .config("retention.ms", "86400000") // 保留24小时
                .config("segment.bytes", "104857600") // 100MB段大小
                .build();
    }

    // 创建缓存更新主题
    @Bean
    public NewTopic cacheUpdateTopic(){
        return TopicBuilder.name(CACHE_UPDATE_TOPIC)
                .partitions(3)
                .replicas(1)
                .config("retention.ms", "43200000") // 12小时
                .build();
    }

    // 创建缓存清理主题
    @Bean
    public NewTopic cacheCleanTopic(){
        return TopicBuilder.name(CACHE_CLEAN_TOPIC)
                .partitions(2)
                .replicas(1)
                .config("retention.ms", "86400000") // 24小时
                .build();
    }
}
