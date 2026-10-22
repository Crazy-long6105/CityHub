package com.hmdp.service.cache;

import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.utils.CacheClient;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TwoLevelCacheModuleTest {
    @Test
    void localHitsCopiesInvalidationAndFailureIsolation() {
        CacheClient redis = mock(CacheClient.class);
        ShopMapper database = mock(ShopMapper.class);
        ShopDetailCache cache = new ShopDetailCache(redis, database, mock(PlatformTransactionManager.class));
        Shop original = new Shop().setId(1L).setName("shop").setDistance(42D);
        when(redis.queryWithLogicalExpire(eq("cache:shop:"), eq(1L), eq(Shop.class), any(), eq(30L), eq(TimeUnit.MINUTES)))
                .thenReturn(original);
        Shop first = cache.query(1L);
        first.setName("changed by caller");
        assertEquals("shop", cache.query(1L).getName());
        assertNull(first.getDistance());
        verify(redis, times(1)).queryWithLogicalExpire(anyString(), eq(1L), eq(Shop.class), any(), anyLong(), any());
        cache.invalidate(1L);
        cache.invalidate(1L);
        assertEquals("shop", cache.query(1L).getName());
        verify(redis, times(2)).queryWithLogicalExpire(anyString(), eq(1L), eq(Shop.class), any(), anyLong(), any());

        when(redis.queryWithLogicalExpire(anyString(), eq(2L), eq(Shop.class), any(), anyLong(), any()))
                .thenThrow(new CacheClient.Unavailable("offline"));
        assertThrows(CacheClient.Unavailable.class, () -> cache.query(2L));
        assertThrows(CacheClient.Unavailable.class, () -> cache.query(2L));
        verify(redis, times(2)).queryWithLogicalExpire(anyString(), eq(2L), eq(Shop.class), any(), anyLong(), any());
        assertEquals("shop", cache.query(1L).getName());
        verifyNoInteractions(database);
    }
}
