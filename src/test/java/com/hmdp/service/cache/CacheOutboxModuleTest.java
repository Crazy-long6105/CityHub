package com.hmdp.service.cache;

import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.CacheManager;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CacheOutboxModuleTest {
    @Test
    @SuppressWarnings("unchecked")
    void transactionRollbackAndTokenGuardedRecovery() {
        PlatformTransactionManager manager = mock(PlatformTransactionManager.class);
        when(manager.getTransaction(any(TransactionDefinition.class))).thenAnswer(call -> new SimpleTransactionStatus());
        ShopMapper shops = mock(ShopMapper.class);
        ShopCacheOutbox mockOutbox = mock(ShopCacheOutbox.class);
        Shop shop = new Shop().setId(1L).setName("updated");
        when(shops.selectById(1L)).thenReturn(new Shop().setId(1L));
        when(shops.updateById(shop)).thenReturn(1);
        when(mockOutbox.append(1L)).thenThrow(new IllegalStateException("outbox unavailable"));
        ShopWriteService writes = new ShopWriteService(shops, mockOutbox, manager);
        assertThrows(IllegalStateException.class, () -> writes.update(shop));
        verify(manager).rollback(any());
        verify(mockOutbox, never()).afterCommit(anyString(), anyLong());
        doReturn("committed-event").when(mockOutbox).append(1L);
        assertTrue(writes.update(shop));
        org.mockito.InOrder commitOrder = inOrder(manager, mockOutbox);
        commitOrder.verify(manager).commit(any());
        commitOrder.verify(mockOutbox).afterCommit("committed-event", 1L);

        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        CacheClient cache = mock(CacheClient.class);
        ShopDetailCache local = mock(ShopDetailCache.class);
        ShopCacheOutbox outbox = new ShopCacheOutbox(jdbc, cache, local, mock(KafkaTemplate.class), Runnable::run, manager);
        when(jdbc.update(anyString(), any(), any())).thenReturn(1);
        when(jdbc.update(anyString(), any(), any(), any())).thenReturn(1);
        outbox.execute("event", 1L);
        verify(cache).invalidateShop(1L);
        verify(jdbc).update(contains("status='DONE'"), eq("event"), anyString());

        doThrow(new CacheClient.Unavailable("offline")).when(cache).invalidateShop(1L);
        outbox.execute("retry", 1L);
        verify(jdbc).update(contains("retry_count=retry_count+1"), anyString(), eq("retry"), anyString());

        when(jdbc.update(contains("status='RUNNING',claim_token"), anyString(), eq("done"), eq(1L))).thenReturn(0);
        Map<String, Object> done = new HashMap<>();
        done.put("status", "DONE");
        done.put("business_id", 1L);
        when(jdbc.queryForList(anyString(), eq("done"))).thenReturn(Collections.singletonList(done));
        outbox.execute("done", 1L);
        verify(cache, times(2)).invalidateShop(1L);
        assertFalse(CacheManager.supportsLegacyKey("cache:shop:1"));
        assertFalse(CacheManager.supportsLegacyKey("seckill:stock:1"));
        assertTrue(CacheManager.supportsLegacyKey("voucher:1"));
        assertThrows(ShopCacheOutbox.InvalidEvent.class, () -> outbox.parse("{\"version\":9}"));
    }
}
