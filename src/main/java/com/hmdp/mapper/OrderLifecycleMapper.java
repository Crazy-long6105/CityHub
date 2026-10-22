package com.hmdp.mapper;

import com.hmdp.entity.OrderCompensationTask;
import com.hmdp.entity.VoucherOrder;
import org.apache.ibatis.annotations.*;

import java.util.List;

public interface OrderLifecycleMapper {
    @Update("UPDATE tb_voucher_order SET status=2,pay_time=CURRENT_TIMESTAMP(3),update_time=CURRENT_TIMESTAMP(3) " +
            "WHERE id=#{orderId} AND user_id=#{userId} AND status=1 AND pay_expire_time>CURRENT_TIMESTAMP(3)")
    @Options(timeout = 5)
    int pay(@Param("orderId") Long orderId, @Param("userId") Long userId);

    @Select("SELECT * FROM tb_voucher_order WHERE id=#{orderId} AND user_id=#{userId}")
    @Options(timeout = 5)
    VoucherOrder findOwned(@Param("orderId") Long orderId, @Param("userId") Long userId);

    @Select("SELECT * FROM tb_voucher_order WHERE id=#{orderId}")
    @Options(timeout = 5)
    VoucherOrder find(Long orderId);

    @Select("SELECT id FROM tb_voucher_order WHERE status=1 AND pay_expire_time<=CURRENT_TIMESTAMP(3) " +
            "ORDER BY pay_expire_time,id LIMIT #{limit}")
    @Options(timeout = 5)
    List<Long> expiredOrders(int limit);

    @Update("UPDATE tb_voucher_order SET status=4,update_time=CURRENT_TIMESTAMP(3) " +
            "WHERE id=#{orderId} AND status=1 AND pay_expire_time<=CURRENT_TIMESTAMP(3)")
    @Options(timeout = 5)
    int close(Long orderId);

    @Update("UPDATE tb_seckill_voucher SET stock=stock+1 WHERE voucher_id=#{voucherId}")
    @Options(timeout = 5)
    int restoreStock(Long voucherId);

    @Delete("DELETE FROM tb_voucher_purchase_qualification WHERE order_id=#{id} " +
            "AND user_id=#{userId} AND voucher_id=#{voucherId}")
    @Options(timeout = 5)
    int deleteQualification(VoucherOrder order);

    @Insert("INSERT INTO tb_order_compensation_task(order_id,user_id,voucher_id,task_type,status,retry_count," +
            "next_execute_time,create_time,update_time) VALUES(#{id},#{userId},#{voucherId},'CLOSE_RELEASE',0,0," +
            "CURRENT_TIMESTAMP(3),CURRENT_TIMESTAMP(3),CURRENT_TIMESTAMP(3))")
    @Options(timeout = 5)
    int insertReleaseTask(VoucherOrder order);

    @Select("SELECT COUNT(*) FROM tb_order_compensation_task WHERE order_id=#{orderId} " +
            "AND task_type='CLOSE_RELEASE' AND status<>2")
    @Options(timeout = 5)
    int pendingRelease(Long orderId);

    @Select("SELECT * FROM tb_order_compensation_task WHERE status=0 AND next_execute_time<=CURRENT_TIMESTAMP(3) " +
            "ORDER BY next_execute_time,id LIMIT #{limit}")
    @Options(timeout = 5)
    List<OrderCompensationTask> dueTasks(int limit);

    @Select("SELECT * FROM tb_order_compensation_task WHERE status=1 AND lease_until<=CURRENT_TIMESTAMP(3) " +
            "ORDER BY lease_until,id LIMIT #{limit}")
    @Options(timeout = 5)
    List<OrderCompensationTask> abandonedTasks(int limit);

    @Update("UPDATE tb_order_compensation_task SET status=1,claim_token=#{token}," +
            "lease_until=TIMESTAMPADD(SECOND,#{leaseSeconds},CURRENT_TIMESTAMP(3)),update_time=CURRENT_TIMESTAMP(3) " +
            "WHERE id=#{id} AND ((status=0 AND next_execute_time<=CURRENT_TIMESTAMP(3)) " +
            "OR (status=1 AND lease_until<=CURRENT_TIMESTAMP(3)))")
    @Options(timeout = 5)
    int claim(@Param("id") Long id, @Param("token") String token, @Param("leaseSeconds") int leaseSeconds);

    @Update("UPDATE tb_order_compensation_task SET status=2,completed_time=CURRENT_TIMESTAMP(3)," +
            "update_time=CURRENT_TIMESTAMP(3),lease_until=NULL,claim_token=NULL,last_error=NULL " +
            "WHERE id=#{id} AND status=1 AND claim_token=#{token}")
    @Options(timeout = 5)
    int complete(@Param("id") Long id, @Param("token") String token);

    @Update("UPDATE tb_order_compensation_task SET status=0,retry_count=retry_count+1," +
            "next_execute_time=TIMESTAMPADD(SECOND,#{delaySeconds},CURRENT_TIMESTAMP(3))," +
            "last_error=#{error},lease_until=NULL,claim_token=NULL,update_time=CURRENT_TIMESTAMP(3) " +
            "WHERE id=#{id} AND status=1 AND claim_token=#{token}")
    @Options(timeout = 5)
    int retry(@Param("id") Long id, @Param("token") String token,
              @Param("delaySeconds") int delaySeconds, @Param("error") String error);
}
