package com.hmdp.service.order;

import com.hmdp.dto.OrderDetailDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.OrderCompensationTask;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.OrderLifecycleMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PaymentModuleTest {
    private OrderLifecycleMapper mapper;
    private SeckillRedisStore redis;
    private PlatformTransactionManager transactions;
    private OrderLifecycleService service;

    @BeforeEach
    void setUp() {
        mapper = mock(OrderLifecycleMapper.class);
        redis = mock(SeckillRedisStore.class);
        transactions = mock(PlatformTransactionManager.class);
        when(transactions.getTransaction(any(TransactionDefinition.class))).thenReturn(new SimpleTransactionStatus());
        service = new OrderLifecycleService(mapper, redis, transactions);
    }

    @Test
    void paymentUsesOwnershipAndCommittedStateForRetriesAndUnknownCommit() {
        VoucherOrder order = order(2);
        LocalDateTime deadline = LocalDateTime.of(2026, 10, 20, 12, 0);
        order.setPayExpireTime(deadline);
        when(mapper.findOwned(10L, 20L)).thenReturn(order);
        Result<OrderDetailDTO> result = service.pay(10L, 20L);
        assertTrue(result.getSuccess());
        assertEquals(deadline, result.getData().getPayExpireTime());
        verify(mapper).pay(10L, 20L);
        verify(transactions).commit(any());

        assertFalse(service.pay(10L, 99L).getSuccess());
        when(mapper.findOwned(10L, 20L)).thenReturn(order(1));
        assertFalse(service.pay(10L, 20L).getSuccess());
        when(mapper.findOwned(10L, 20L)).thenReturn(order(7));
        assertFalse(service.pay(10L, 20L).getSuccess());

        when(mapper.findOwned(10L, 20L)).thenReturn(order);
        doThrow(new IllegalStateException("commit acknowledgement lost")).when(transactions).commit(any());
        Result<OrderDetailDTO> unknown = service.pay(10L, 20L);
        assertFalse(unknown.getSuccess());
        assertTrue(unknown.getErrorMsg().contains("pending"));
        verifyNoInteractions(redis);
    }

    @Test
    void onlyWinningCloseRestoresStockAndMissingQualificationRollsBack() {
        assertFalse(service.closeExpired(10L));
        verify(mapper, never()).restoreStock(anyLong());
        verify(mapper, never()).insertReleaseTask(any());

        when(mapper.close(10L)).thenReturn(1);
        VoucherOrder order = order(4);
        when(mapper.find(10L)).thenReturn(order);
        when(mapper.restoreStock(30L)).thenReturn(1);
        assertThrows(IllegalStateException.class, () -> service.closeExpired(10L));
        verify(transactions).rollback(any());
        verify(mapper, never()).insertReleaseTask(any());

        when(mapper.deleteQualification(order)).thenReturn(1);
        when(mapper.insertReleaseTask(order)).thenReturn(1);
        assertTrue(service.closeExpired(10L));
        when(mapper.close(10L)).thenReturn(0);
        assertFalse(service.closeExpired(10L));
        verify(mapper, times(2)).restoreStock(30L);
        verify(mapper).insertReleaseTask(order);
        verifyNoInteractions(redis);
    }

    @Test
    void failedReleaseKeepsTaskAndStaleLeaseCannotExpireReleaseMarker() {
        OrderCompensationTask task = task();
        when(mapper.abandonedTasks(100)).thenReturn(Collections.singletonList(task));
        when(mapper.dueTasks(99)).thenReturn(Collections.emptyList());
        when(mapper.claim(eq(1L), anyString(), eq(30))).thenReturn(1);
        doThrow(new IllegalStateException("Redis unavailable")).when(redis).release(10L, 20L, 30L);
        service.releaseBatch(100, 30);
        verify(mapper).retry(eq(1L), anyString(), eq(1), contains("Redis unavailable"));
        verify(mapper, never()).complete(anyLong(), anyString());

        doNothing().when(redis).release(10L, 20L, 30L);
        service.releaseBatch(100, 30);
        verify(mapper).complete(eq(1L), anyString());
        verify(redis, never()).expireReleaseMarker(anyLong());

        when(mapper.complete(eq(1L), anyString())).thenReturn(1);
        service.releaseBatch(100, 30);
        verify(redis).expireReleaseMarker(10L);
        assertEquals(Arrays.asList(1, 5, 30, 60, 60), Arrays.asList(
                OrderLifecycleService.retryDelay(0), OrderLifecycleService.retryDelay(1),
                OrderLifecycleService.retryDelay(2), OrderLifecycleService.retryDelay(3),
                OrderLifecycleService.retryDelay(100)));
    }

    @Test
    void closedOrderDistinguishesPendingReleaseAndLostClaimsDoNoWork() {
        when(mapper.findOwned(10L, 20L)).thenReturn(order(4));
        when(mapper.pendingRelease(10L)).thenReturn(1);
        assertTrue(service.detail(10L, 20L).getData().isReleasePending());
        when(mapper.pendingRelease(10L)).thenReturn(0);
        assertFalse(service.detail(10L, 20L).getData().isReleasePending());
        assertFalse(service.detail(10L, 99L).getSuccess());
        when(mapper.abandonedTasks(100)).thenReturn(Collections.emptyList());
        when(mapper.dueTasks(100)).thenReturn(Collections.singletonList(task()));
        service.releaseBatch(1000, 30);
        verify(mapper).dueTasks(100);
        verifyNoInteractions(redis);
    }

    private VoucherOrder order(int status) {
        VoucherOrder order = new VoucherOrder();
        order.setId(10L);
        order.setUserId(20L);
        order.setVoucherId(30L);
        order.setStatus(status);
        return order;
    }

    private OrderCompensationTask task() {
        OrderCompensationTask task = new OrderCompensationTask();
        task.setId(1L);
        task.setOrderId(10L);
        task.setUserId(20L);
        task.setVoucherId(30L);
        task.setRetryCount(0);
        return task;
    }
}
