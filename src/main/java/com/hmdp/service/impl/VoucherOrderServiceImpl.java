package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.config.KafkaConfig;
import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillMessage;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.service.order.SeckillOrderTransactions;
import com.hmdp.service.order.SeckillQueryToken;
import com.hmdp.service.order.SeckillRedisStore;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource private SeckillRedisStore store;
    @Resource private SeckillQueryToken tokens;
    @Resource private SeckillOrderTransactions transactions;
    @Resource private RedisIdWorker redisIdWorker;
    @Resource private KafkaTemplate<String, Object> kafkaTemplate;
    @Resource private IVoucherService voucherService;
    @Value("${hmdp.seckill.enabled:false}") private boolean enabled;

    @javax.annotation.PostConstruct
    public void validateEnabledConfiguration() {
        if (enabled) tokens.issue(1L, 1L, 1, 2);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        if (!enabled) return Result.fail("秒杀暂未开放");
        if (voucherId == null || voucherId <= 0) return Result.fail("优惠券参数错误");
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        long permit = System.currentTimeMillis() + 1000;
        long expiry = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(24);
        String token = tokens.issue(orderId, userId, permit, expiry);
        List<String> reserved;
        try { reserved = store.reserve(voucherId, userId, orderId, permit, expiry); }
        catch (RuntimeException e) {
            log.warn("Reservation confirmation unavailable, order={}", orderId);
            return response(orderId, token, "ACCEPTANCE_UNKNOWN", "受理未确认，请查询结果");
        }
        String code = reserved.get(0);
        if ("EXISTING".equals(code)) {
            orderId = Long.parseLong(reserved.get(1));
            // An expired request must never be recreated merely to extend a credential.
            SeckillRedisStore.Snapshot existing = store.read(orderId);
            if (existing.missing()) return response(orderId, null, "RECORD_EXPIRED", "请查询订单详情");
            expiry = Long.parseLong(existing.fields.get("credentialExpiry"));
            token = tokens.issue(orderId, userId, permit, expiry);
        } else if (!"ACCEPTED".equals(code)) {
            return Result.fail("秒杀未受理: " + code);
        }
        try {
            SeckillRedisStore.Snapshot snapshot = store.read(orderId);
            if (!snapshot.terminal()) send(snapshot.message(), true);
        } catch (RuntimeException e) { log.warn("Request remains recoverable, order={}", orderId); }
        return queryRequest(orderId, token);
    }

    @Override
    public Result queryRequest(Long orderId, String token) {
        Long userId = UserHolder.getUser().getId();
        final long[] credential;
        try { credential = tokens.verify(token, orderId, userId); }
        catch (IllegalArgumentException e) { return Result.fail("查询凭据无效"); }
        try {
            SeckillRedisStore.Snapshot snapshot = store.read(orderId);
            if (snapshot.now >= credential[1]) return response(orderId, token, "RECORD_EXPIRED", "查询凭据已过期");
            if (snapshot.missing()) return response(orderId, token,
                    snapshot.now > credential[0] ? "NOT_ACCEPTED" : "ACCEPTANCE_UNKNOWN", "");
            if (!userId.toString().equals(snapshot.fields.get("userId"))) return Result.fail("请求不存在");
            String status = snapshot.fields.get("status");
            if (!snapshot.terminal() && snapshot.now >= Long.parseLong(snapshot.fields.get("deadline"))) status = "CONFIRMING";
            else if (!snapshot.terminal() && snapshot.now - Long.parseLong(snapshot.fields.get("createdAt")) >= 10000) status = "SLOW";
            return response(orderId, token, status, snapshot.fields.get("reason"));
        } catch (RuntimeException e) { return response(orderId, token, "CONFIRMING", "结果暂未确认，请稍后查询"); }
    }

    private Result response(Long order, String token, String status, String reason) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("orderId", order.toString());
        data.put("queryToken", token);
        data.put("status", status);
        data.put("reason", reason);
        return Result.ok(data);
    }

    public void send(SeckillMessage message, boolean waitForConfirmation) {
        if (!store.beginSend(message.getOrderId())) return;
        long budget = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        try {
            org.springframework.util.concurrent.ListenableFuture<?> future = kafkaTemplate.send(
                    KafkaConfig.SECKILL_ORDER_TOPIC, message.getOrderId().toString(), message);
            future.addCallback(value -> {
                try { store.acknowledgeSend(message.getOrderId()); }
                catch (RuntimeException e) { log.warn("Send confirmation storage failed, order={}", message.getOrderId()); }
            }, error -> log.warn("Send outcome unknown, order={}", message.getOrderId()));
            if (waitForConfirmation) future.get(Math.max(1, budget - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) { log.warn("Send remains unconfirmed, order={}", message.getOrderId()); }
    }

    @Override
    public void createVoucherOrder(SeckillMessage incoming) {
        if (incoming == null || incoming.getOrderId() == null) throw new IllegalArgumentException("Invalid order message");
        SeckillRedisStore.Snapshot snapshot = store.read(incoming.getOrderId());
        if (snapshot.missing()) throw new IllegalStateException("Missing request; quarantine message and audit control data");
        SeckillMessage expected = snapshot.message();
        if (!Objects.equals(expected, incoming)) throw new IllegalArgumentException("Order message identity mismatch");
        if (snapshot.terminal()) return;
        if (snapshot.now >= expected.getDeadline()) { terminateAndFinish(expected, "Creation deadline exceeded"); return; }
        try {
            transactions.create(expected);
        } catch (DuplicateKeyException e) {
            VoucherOrder existing = getById(expected.getOrderId());
            if (existing != null) { finishFromDatabase(expected, existing); return; }
            terminateAndFinish(expected, "Purchase qualification already occupied");
            return;
        } catch (SeckillOrderTransactions.StockUnavailableException e) {
            terminateAndFinish(expected, "Database stock unavailable");
            return;
        }
        store.finish(expected.getOrderId(), true, "");
    }

    private void finishFromDatabase(SeckillMessage message, VoucherOrder order) {
        if (!Objects.equals(order.getUserId(), message.getUserId()) || !Objects.equals(order.getVoucherId(), message.getVoucherId()))
            throw new IllegalStateException("Order identity mismatch");
        if (Integer.valueOf(7).equals(order.getStatus())) {
            store.release(message.getOrderId(), message.getUserId(), message.getVoucherId());
            store.finish(message.getOrderId(), false, order.getFailureReason());
            store.expireReleaseMarker(message.getOrderId());
        } else {
            store.finish(message.getOrderId(), true, "");
        }
    }

    private void terminateAndFinish(SeckillMessage message, String reason) {
        try { transactions.terminate(message, reason); }
        catch (DuplicateKeyException e) {
            VoucherOrder existing = getById(message.getOrderId());
            if (existing == null) throw new IllegalStateException("Conflicting transaction not visible", e);
            finishFromDatabase(message, existing);
            return;
        }
        store.release(message.getOrderId(), message.getUserId(), message.getVoucherId());
        store.finish(message.getOrderId(), false, reason);
        store.expireReleaseMarker(message.getOrderId());
    }

    public void recover(Long orderId) {
        SeckillRedisStore.Snapshot snapshot = store.read(orderId);
        if (snapshot.missing()) throw new IllegalStateException("Recovery request missing; audit required");
        if (snapshot.terminal()) return;
        SeckillMessage message = snapshot.message();
        if (snapshot.now >= message.getDeadline()) {
            terminateAndFinish(message, "Creation deadline exceeded");
        } else {
            send(message, false);
            store.retryLater(orderId, 5000);
        }
    }

    @Override
    public void addSeckillVoucher(Voucher voucher) { voucherService.addSeckillVoucher(voucher); }
}
