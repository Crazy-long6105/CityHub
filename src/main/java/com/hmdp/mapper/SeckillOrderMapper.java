package com.hmdp.mapper;

import com.hmdp.dto.SeckillMessage;
import org.apache.ibatis.annotations.*;

public interface SeckillOrderMapper {
    @Insert("INSERT INTO tb_voucher_order(id,user_id,voucher_id,status,pay_expire_time) " +
            "VALUES(#{m.orderId},#{m.userId},#{m.voucherId},1,TIMESTAMPADD(SECOND,#{seconds},CURRENT_TIMESTAMP(3)))")
    int insertPending(@Param("m") SeckillMessage message, @Param("seconds") int seconds);

    @Insert("INSERT INTO tb_voucher_purchase_qualification(user_id,voucher_id,order_id) " +
            "VALUES(#{userId},#{voucherId},#{orderId})")
    int insertQualification(SeckillMessage message);

    @Update("UPDATE tb_seckill_voucher SET stock=stock-1 WHERE voucher_id=#{id} AND stock>0")
    int deductStock(Long id);

    @Insert("INSERT INTO tb_voucher_order(id,user_id,voucher_id,status,failure_reason) " +
            "VALUES(#{m.orderId},#{m.userId},#{m.voucherId},7,#{reason})")
    int insertTerminated(@Param("m") SeckillMessage message, @Param("reason") String reason);
}
