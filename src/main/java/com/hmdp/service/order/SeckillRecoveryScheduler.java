package com.hmdp.service.order;

import com.hmdp.service.impl.VoucherOrderServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@EnableScheduling
@ConditionalOnProperty(name = "hmdp.seckill.enabled", havingValue = "true")
public class SeckillRecoveryScheduler {
    private final SeckillRedisStore store;
    private final VoucherOrderServiceImpl orders;
    public SeckillRecoveryScheduler(SeckillRedisStore store, VoucherOrderServiceImpl orders) {
        this.store = store; this.orders = orders;
    }
    @Scheduled(fixedDelayString = "${hmdp.seckill.recovery-delay-ms:1000}")
    public void recover() {
        try {
            // Claim no more work than this single worker can begin immediately.
            for (Long id : store.claim(1)) {
                try { orders.recover(id); }
                catch (RuntimeException e) {
                    log.error("Order recovery deferred, order={}", id, e);
                    store.retryLater(id, 60000);
                }
            }
        } catch (RuntimeException e) { log.error("Order recovery storage unavailable", e); }
    }
}
