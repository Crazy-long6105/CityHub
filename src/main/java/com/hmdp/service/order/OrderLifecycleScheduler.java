package com.hmdp.service.order;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@EnableScheduling
@ConditionalOnProperty(name = "hmdp.order.lifecycle.enabled", havingValue = "true")
public class OrderLifecycleScheduler {
    private final OrderLifecycleService service;
    @Value("${hmdp.order.lifecycle.batch-size:100}")
    private int batchSize;
    @Value("${hmdp.order.lifecycle.lease-seconds:30}")
    private int leaseSeconds;

    public OrderLifecycleScheduler(OrderLifecycleService service) {
        this.service = service;
    }

    @Scheduled(fixedDelayString = "${hmdp.order.lifecycle.close-delay-ms:5000}")
    public void closeExpiredOrders() {
        try {
            service.closeBatch(batchSize);
        } catch (RuntimeException e) {
            log.error("Order expiry scan failed", e);
        }
    }

    @Scheduled(fixedDelayString = "${hmdp.order.lifecycle.release-delay-ms:1000}")
    public void releaseClosedOrders() {
        try {
            service.releaseBatch(batchSize, leaseSeconds);
        } catch (RuntimeException e) {
            log.error("Order release scan failed", e);
        }
    }
}
