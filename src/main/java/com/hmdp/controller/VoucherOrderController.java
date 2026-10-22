package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.order.OrderLifecycleService;
import com.hmdp.utils.UserHolder;
import com.hmdp.utils.ratelimiter.RateLimiter;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private OrderLifecycleService lifecycle;

    /**
     * 秒杀优惠券
     * @param voucherId 优惠券ID
     * @return 订单ID
     */
    @RateLimiter(
            dimensions = {RateLimiter.LimitType.METHOD, RateLimiter.LimitType.IP, RateLimiter.LimitType.USER},
            highRisk = true,
            requireLogin = true,
            window = 10,
            limit = 10,
            message = "秒杀活动太火爆，请稍后再试"
    )
    @PostMapping("seckill/{id}")
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        return voucherOrderService.seckillVoucher(voucherId);
    }

    @RateLimiter(type = RateLimiter.LimitType.USER, limit = 20, window = 10, requireLogin = true)
    @GetMapping("/requests/{id}")
    public Result queryRequest(@PathVariable("id") Long id, @RequestParam String token) {
        return voucherOrderService.queryRequest(id, token);
    }

    @RateLimiter(type = RateLimiter.LimitType.USER, limit = 10, window = 10, highRisk = true, requireLogin = true)
    @PostMapping("/{id}/pay")
    public Result pay(@PathVariable Long id) { return lifecycle.pay(id, UserHolder.getUser().getId()); }

    @RateLimiter(type = RateLimiter.LimitType.USER, limit = 10, window = 10, requireLogin = true)
    @GetMapping("/{id}")
    public Result detail(@PathVariable Long id) { return lifecycle.detail(id, UserHolder.getUser().getId()); }
}
