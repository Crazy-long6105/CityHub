package com.hmdp.service.order;

import com.hmdp.dto.OrderDetailDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.OrderCompensationTask;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.OrderLifecycleMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
public class OrderLifecycleService {
    private final OrderLifecycleMapper mapper;
    private final SeckillRedisStore redisStore;
    private final TransactionTemplate transaction;

    public OrderLifecycleService(OrderLifecycleMapper mapper, SeckillRedisStore redisStore,
                                 PlatformTransactionManager transactionManager) {
        this.mapper = mapper;
        this.redisStore = redisStore;
        transaction = new TransactionTemplate(transactionManager);
        transaction.setTimeout(5);
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public Result<OrderDetailDTO> pay(Long orderId, Long userId) {
        if (orderId == null || userId == null) {
            return Result.fail("Login and order id required");
        }
        try {
            // Return only after commit; a lost commit acknowledgement must be reconciled by the caller.
            return transaction.execute(status -> {
                mapper.pay(orderId, userId);
                VoucherOrder order = mapper.findOwned(orderId, userId);
                if (order == null) {
                    return Result.fail("Order unavailable");
                }
                if (!Integer.valueOf(2).equals(order.getStatus())) {
                    return Result.fail("Order cannot be paid or payment deadline has passed");
                }
                return Result.ok(toDetail(order));
            });
        } catch (RuntimeException e) {
            log.error("Payment result requires reconciliation, order={}", orderId, e);
            return Result.fail("Payment result pending; query this order before retrying");
        }
    }

    public Result<OrderDetailDTO> detail(Long orderId, Long userId) {
        if (orderId == null || userId == null) {
            return Result.fail("Login and order id required");
        }
        VoucherOrder order = mapper.findOwned(orderId, userId);
        return order == null ? Result.fail("Order unavailable") : Result.ok(toDetail(order));
    }

    private OrderDetailDTO toDetail(VoucherOrder order) {
        OrderDetailDTO detail = new OrderDetailDTO();
        detail.setId(order.getId());
        detail.setStatus(order.getStatus());
        detail.setPayTime(order.getPayTime());
        detail.setPayExpireTime(order.getPayExpireTime());
        detail.setReleasePending(Integer.valueOf(4).equals(order.getStatus()) &&
                mapper.pendingRelease(order.getId()) > 0);
        return detail;
    }

    public boolean closeExpired(Long orderId) {
        return Boolean.TRUE.equals(transaction.execute(status -> {
            if (mapper.close(orderId) == 0) {
                return false;
            }
            VoucherOrder order = mapper.find(orderId);
            if (order == null || mapper.restoreStock(order.getVoucherId()) != 1 ||
                    mapper.deleteQualification(order) != 1 || mapper.insertReleaseTask(order) != 1) {
                throw new IllegalStateException("Close consistency check failed for order " + orderId);
            }
            return true;
        }));
    }

    public void closeBatch(int batchSize) {
        for (Long id : mapper.expiredOrders(boundedBatchSize(batchSize))) {
            try {
                closeExpired(id);
            } catch (RuntimeException e) {
                log.error("Order close failed; transaction rolled back, order={}", id, e);
            }
        }
    }

    public void releaseBatch(int batchSize, int leaseSeconds) {
        int limit = boundedBatchSize(batchSize);
        List<OrderCompensationTask> tasks = new ArrayList<>(mapper.abandonedTasks(limit));
        if (tasks.size() < limit) {
            tasks.addAll(mapper.dueTasks(limit - tasks.size()));
        }
        // Claim immediately before execution: no leased work waits in a local queue.
        for (OrderCompensationTask task : tasks) {
            release(task, Math.max(5, Math.min(300, leaseSeconds)));
        }
    }

    private void release(OrderCompensationTask task, int leaseSeconds) {
        String token = UUID.randomUUID().toString();
        try {
            if (mapper.claim(task.getId(), token, leaseSeconds) != 1) {
                return;
            }
            redisStore.release(task.getOrderId(), task.getUserId(), task.getVoucherId());
            if (mapper.complete(task.getId(), token) == 1) {
                try {
                    redisStore.expireReleaseMarker(task.getOrderId());
                } catch (RuntimeException markerError) {
                    log.warn("Completed release marker retained without expiry, order={}", task.getOrderId(), markerError);
                }
            }
        } catch (RuntimeException e) {
            log.error("Redis release deferred, order={}, retry={}", task.getOrderId(), task.getRetryCount(), e);
            try {
                mapper.retry(task.getId(), token, retryDelay(task.getRetryCount()), errorSummary(e));
            } catch (RuntimeException retryError) {
                log.error("Release lease will recover after retry update failed, task={}", task.getId(), retryError);
            }
        }
    }

    static int retryDelay(Integer retries) {
        int count = retries == null ? 0 : retries;
        return count <= 0 ? 1 : count == 1 ? 5 : count == 2 ? 30 : 60;
    }

    private static int boundedBatchSize(int configured) {
        return Math.max(1, Math.min(100, configured));
    }

    private static String errorSummary(RuntimeException error) {
        String summary = error.getClass().getSimpleName() + ": " + error.getMessage();
        return summary.substring(0, Math.min(500, summary.length()));
    }
}
