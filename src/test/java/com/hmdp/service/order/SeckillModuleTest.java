package com.hmdp.service.order;

import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillMessage;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SeckillModuleTest {
    private final SeckillRedisStore store = mock(SeckillRedisStore.class);
    private final SeckillOrderTransactions transactions = mock(SeckillOrderTransactions.class);
    private final VoucherOrderMapper mapper = mock(VoucherOrderMapper.class);
    private final SeckillQueryToken tokens = new SeckillQueryToken("test-secret-with-at-least-32-bytes-long");

    private VoucherOrderServiceImpl service() {
        VoucherOrderServiceImpl service = new VoucherOrderServiceImpl();
        ReflectionTestUtils.setField(service, "store", store);
        ReflectionTestUtils.setField(service, "transactions", transactions);
        ReflectionTestUtils.setField(service, "baseMapper", mapper);
        ReflectionTestUtils.setField(service, "tokens", tokens);
        UserDTO user = new UserDTO(); user.setId(2L); UserHolder.saveUser(user);
        return service;
    }
    @AfterEach void clearUser() { UserHolder.removeUser(); }

    @Test void queryCredentialAndUnknownReservationNeverGuessFailureOrQueryDatabase() {
        VoucherOrderServiceImpl service = service();
        String credential = tokens.issue(1L, 2L, 1000, 100000);
        assertThrows(IllegalArgumentException.class, () -> tokens.verify(credential, 1L, 3L));
        assertThrows(IllegalArgumentException.class, () -> tokens.verify(credential.replace(":1000:", ":2000:"), 1L, 2L));
        when(store.read(1L)).thenReturn(new SeckillRedisStore.Snapshot(1000, Collections.emptyMap()));
        assertEquals("ACCEPTANCE_UNKNOWN", status(service.queryRequest(1L, credential)));
        when(store.read(1L)).thenReturn(new SeckillRedisStore.Snapshot(1001, Collections.emptyMap()));
        assertEquals("NOT_ACCEPTED", status(service.queryRequest(1L, credential)));
        when(store.read(1L)).thenReturn(new SeckillRedisStore.Snapshot(100000, Collections.emptyMap()));
        assertEquals("RECORD_EXPIRED", status(service.queryRequest(1L, credential)));
        verifyNoInteractions(mapper, transactions);
    }

    @Test void duplicateCreatedOrderDoesNotDeductAgainAndLifecycleIsPreserved() {
        VoucherOrderServiceImpl service = service();
        SeckillRedisStore.Snapshot snapshot = snapshot(500);
        when(store.read(1L)).thenReturn(snapshot);
        doThrow(new DuplicateKeyException("same primary key")).when(transactions).create(any());
        VoucherOrder paid = new VoucherOrder().setId(1L).setUserId(2L).setVoucherId(3L).setStatus(2);
        when(mapper.selectById(1L)).thenReturn(paid);
        service.createVoucherOrder(snapshot.message());
        verify(store).finish(1L, true, "");
        verify(store, never()).release(any(), any(), any());
        verify(transactions, never()).terminate(any(), anyString());
        assertEquals(2, paid.getStatus());
    }

    @Test void expiredCreationNeedsCommittedTerminationBeforeReleaseAndRejectsForgedMessage() {
        VoucherOrderServiceImpl service = service();
        SeckillRedisStore.Snapshot expired = snapshot(31000);
        when(store.read(1L)).thenReturn(expired);
        doThrow(new IllegalStateException("database unavailable")).when(transactions).terminate(any(), anyString());
        assertThrows(IllegalStateException.class, () -> service.createVoucherOrder(expired.message()));
        verify(store, never()).release(any(), any(), any());
        verify(store, never()).finish(any(), anyBoolean(), anyString());
        verify(transactions, never()).create(any());
        reset(transactions);
        service.createVoucherOrder(expired.message());
        org.mockito.InOrder sequence = inOrder(transactions, store);
        sequence.verify(transactions).terminate(any(), anyString());
        sequence.verify(store).release(1L, 2L, 3L);
        sequence.verify(store).finish(eq(1L), eq(false), anyString());
        SeckillMessage forged = expired.message(); forged.setUserId(99L);
        assertThrows(IllegalArgumentException.class, () -> service.createVoucherOrder(forged));
    }

    private static String status(Result result) { return (String) ((Map) result.getData()).get("status"); }
    private static SeckillRedisStore.Snapshot snapshot(long now) {
        Map<String, String> values = new HashMap<>();
        values.put("orderId", "1"); values.put("userId", "2"); values.put("voucherId", "3");
        values.put("createdAt", "1000"); values.put("deadline", "31000"); values.put("status", "PROCESSING");
        return new SeckillRedisStore.Snapshot(now, values);
    }
}
