package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherService;
import lombok.extern.slf4j.Slf4j;
import com.hmdp.service.order.SeckillRedisStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.annotation.Resource;
import java.util.List;
import java.time.ZoneId;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherServiceImpl extends ServiceImpl<VoucherMapper, Voucher> implements IVoucherService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private SeckillRedisStore control;

    @Value("${hmdp.business-zone:Asia/Shanghai}")
    private String businessZone;

    @Override
    public Result queryVoucherOfShop(Long shopId) {
        // 查询优惠券信息
        List<Voucher> vouchers = getBaseMapper().queryVoucherOfShop(shopId);
        // 返回结果
        return Result.ok(vouchers);
    }

    @Override
    @Transactional
    public void addSeckillVoucher(Voucher voucher) {
        if (voucher.getStock() == null || voucher.getStock() < 0 || voucher.getBeginTime() == null ||
                voucher.getEndTime() == null || !voucher.getBeginTime().isBefore(voucher.getEndTime())) {
            throw new IllegalArgumentException("Invalid seckill activity");
        }
        // 保存优惠券
        if (!save(voucher)) throw new IllegalStateException("Voucher insert failed");
        // 保存秒杀信息
        SeckillVoucher seckillVoucher = new SeckillVoucher();
        seckillVoucher.setVoucherId(voucher.getId());
        seckillVoucher.setStock(voucher.getStock());
        seckillVoucher.setBeginTime(voucher.getBeginTime());
        seckillVoucher.setEndTime(voucher.getEndTime());
        if (!seckillVoucherService.save(seckillVoucher)) throw new IllegalStateException("Activity insert failed");
        final long begin = voucher.getBeginTime().atZone(ZoneId.of(businessZone)).toInstant().toEpochMilli();
        final long end = voucher.getEndTime().atZone(ZoneId.of(businessZone)).toInstant().toEpochMilli();
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() {
                try { control.initialize(voucher.getId(), voucher.getStock(), begin, end); }
                catch (RuntimeException e) {
                    // Missing controls fail closed. Operators can initialize only a never-opened activity.
                    log.error("Activity persisted but control initialization requires recovery, voucher={}", voucher.getId(), e);
                }
            }
        });
    }
}
