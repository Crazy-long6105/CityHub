package com.hmdp.service.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.utils.CacheClient;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.TimeUnit;

@Service
public class ShopDetailCache {
    static final class LocalValue {
        final Shop shop;
        LocalValue(Shop shop) { this.shop = copy(shop); }
    }

    private final CacheClient cache;
    private final ShopMapper shops;
    private final TransactionTemplate read;
    private final Cache<Long, LocalValue> local;

    public ShopDetailCache(CacheClient cache, ShopMapper shops, PlatformTransactionManager transactions) {
        this.cache = cache;
        this.shops = shops;
        this.read = new TransactionTemplate(transactions);
        this.read.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.read.setReadOnly(true);
        this.read.setTimeout(2);
        this.local = Caffeine.newBuilder().maximumSize(10000).recordStats()
                .expireAfter(new Expiry<Long, LocalValue>() {
                    @Override public long expireAfterCreate(Long key, LocalValue value, long now) {
                        return TimeUnit.SECONDS.toNanos(value.shop == null ? 2 : 30);
                    }
                    @Override public long expireAfterUpdate(Long key, LocalValue value, long now, long duration) {
                        return expireAfterCreate(key, value, now);
                    }
                    @Override public long expireAfterRead(Long key, LocalValue value, long now, long duration) {
                        return duration;
                    }
                }).build();
    }

    public Shop query(Long id) {
        LocalValue value = local.get(id, key -> new LocalValue(cache.queryWithLogicalExpire(
                "cache:shop:", key, Shop.class,
                shopId -> read.execute(status -> shops.selectById(shopId)), 30L, TimeUnit.MINUTES)));
        return copy(value.shop);
    }

    public void invalidate(Long id) { local.invalidate(id); }

    public String statistics() { return local.stats().toString(); }

    static Shop copy(Shop shop) {
        if (shop == null) return null;
        Shop copy = new Shop();
        BeanUtils.copyProperties(shop, copy);
        copy.setDistance(null);
        return copy;
    }
}
