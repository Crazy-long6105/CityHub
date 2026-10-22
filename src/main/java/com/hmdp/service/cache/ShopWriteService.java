package com.hmdp.service.cache;

import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class ShopWriteService {
    private final ShopMapper shops;
    private final ShopCacheOutbox outbox;
    private final TransactionTemplate transaction;

    public ShopWriteService(ShopMapper shops, ShopCacheOutbox outbox, PlatformTransactionManager manager) {
        this.shops = shops;
        this.outbox = outbox;
        this.transaction = new TransactionTemplate(manager);
        this.transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public boolean update(Shop shop) {
        String task = transaction.execute(status -> {
            if (shops.selectById(shop.getId()) == null) return null;
            int changed = shops.updateById(shop);
            if (changed == 0 && shops.selectById(shop.getId()) == null) {
                throw new IllegalStateException("Shop disappeared during update");
            }
            return outbox.append(shop.getId());
        });
        if (task == null) return false;
        outbox.afterCommit(task, shop.getId());
        return true;
    }

    public boolean create(Shop shop) {
        String task = transaction.execute(status -> {
            if (shops.insert(shop) != 1) throw new IllegalStateException("Shop insert failed");
            return outbox.append(shop.getId());
        });
        outbox.afterCommit(task, shop.getId());
        return true;
    }
}
