package com.hmdp.service.order;

import com.hmdp.dto.SeckillMessage;
import com.hmdp.mapper.SeckillOrderMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class SeckillOrderTransactions {
    private final SeckillOrderMapper mapper;
    private final int paymentSeconds;
    public SeckillOrderTransactions(SeckillOrderMapper mapper,
            @Value("${hmdp.order.pay-seconds:900}") int paymentSeconds) {
        if (paymentSeconds < 1) throw new IllegalArgumentException("Invalid payment lifetime");
        this.mapper = mapper;
        this.paymentSeconds = paymentSeconds;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, timeout = 5)
    public void create(SeckillMessage message) {
        requireOne(mapper.insertPending(message, paymentSeconds));
        requireOne(mapper.insertQualification(message));
        if (mapper.deductStock(message.getVoucherId()) != 1) throw new StockUnavailableException();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, timeout = 5)
    public void terminate(SeckillMessage message, String reason) { requireOne(mapper.insertTerminated(message, reason)); }

    private void requireOne(int affected) {
        if (affected != 1) throw new IllegalStateException("Unexpected database write count");
    }
    public static class StockUnavailableException extends RuntimeException { }
}
