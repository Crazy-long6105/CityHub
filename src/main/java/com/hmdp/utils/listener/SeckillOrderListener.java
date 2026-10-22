package com.hmdp.utils.listener;

import com.hmdp.config.KafkaConfig;
import com.hmdp.dto.SeckillMessage;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import javax.annotation.Resource;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SeckillOrderListener {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @KafkaListener(topics = KafkaConfig.SECKILL_ORDER_TOPIC, groupId = "seckill_group")
    public void listenSeckillOrder(@Payload SeckillMessage message, Acknowledgment ack) {
        voucherOrderService.createVoucherOrder(message);
        ack.acknowledge();
    }
}
