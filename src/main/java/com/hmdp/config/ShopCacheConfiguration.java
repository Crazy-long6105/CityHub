package com.hmdp.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.backoff.FixedBackOff;

import javax.annotation.PreDestroy;

import java.util.HashMap;
import java.util.Map;

@Configuration
@org.springframework.scheduling.annotation.EnableScheduling
public class ShopCacheConfiguration {
    private DefaultKafkaProducerFactory<String, String> cacheProducerFactory;

    public ShopCacheConfiguration(@Value("${hmdp.instance-id}") String instanceId) {
        if (!instanceId.matches("[A-Za-z0-9._-]{1,100}")) {
            throw new IllegalArgumentException("hmdp.instance-id must be a stable, unique deployment identity");
        }
    }
    @Bean("shopCacheExecutor")
    public ThreadPoolTaskExecutor shopCacheExecutor() { return executor(10, 100, "shop-refresh-"); }

    @Bean("shopCacheDispatchExecutor")
    public ThreadPoolTaskExecutor shopCacheDispatchExecutor() { return executor(2, 100, "shop-dispatch-"); }

    private ThreadPoolTaskExecutor executor(int threads, int queue, String prefix) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threads);
        executor.setMaxPoolSize(threads);
        executor.setQueueCapacity(queue);
        executor.setThreadNamePrefix(prefix);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(5);
        return executor;
    }

    @Bean("shopCacheKafkaTemplate")
    public KafkaTemplate<String, String> shopCacheKafkaTemplate(
            @Value("${spring.kafka.bootstrap-servers}") String servers) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1048576);
        cacheProducerFactory = new DefaultKafkaProducerFactory<>(config);
        return new KafkaTemplate<>(cacheProducerFactory);
    }

    @PreDestroy
    public void closeProducer() {
        if (cacheProducerFactory != null) cacheProducerFactory.destroy();
    }

    @Bean("cacheKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> cacheKafkaListenerContainerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String servers) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(config));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setErrorHandler(new SeekToCurrentErrorHandler(new FixedBackOff(1000, Long.MAX_VALUE)));
        return factory;
    }
}
