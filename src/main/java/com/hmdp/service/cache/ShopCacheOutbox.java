package com.hmdp.service.cache;

import cn.hutool.json.JSONUtil;
import com.hmdp.config.KafkaConfig;
import com.hmdp.utils.CacheClient;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class ShopCacheOutbox {
    @Data
    public static class Event {
        private int version = 1;
        private String eventId;
        private Long shopId;
        public Event() { }
        Event(String eventId, Long shopId) { this.eventId = eventId; this.shopId = shopId; }
    }

    public static class InvalidEvent extends RuntimeException {
        public InvalidEvent(String reason) { super(reason); }
    }

    private final JdbcTemplate jdbc;
    private final CacheClient cache;
    private final ShopDetailCache local;
    private final KafkaTemplate<String, String> kafka;
    private final Executor dispatch;
    private final TransactionTemplate stateTransaction;

    public ShopCacheOutbox(JdbcTemplate jdbc, CacheClient cache, ShopDetailCache local,
                          @Qualifier("shopCacheKafkaTemplate") KafkaTemplate<String, String> kafka,
                          @Qualifier("shopCacheDispatchExecutor") Executor dispatch,
                          PlatformTransactionManager transactions) {
        this.jdbc = jdbc;
        this.cache = cache;
        this.local = local;
        this.kafka = kafka;
        this.dispatch = dispatch;
        this.stateTransaction = new TransactionTemplate(transactions);
        this.stateTransaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.stateTransaction.setTimeout(2);
    }

    /** Must be called in the same database transaction as the shop write. */
    public String append(Long shopId) {
        String id = UUID.randomUUID().toString();
        if (jdbc.update("INSERT INTO tb_cache_invalidation_task(id,business_id) VALUES (?,?)", id, shopId) != 1) {
            throw new IllegalStateException("Cache task insert failed");
        }
        return id;
    }

    public void afterCommit(String id, Long shopId) {
        try {
            local.invalidate(shopId);
            execute(id, shopId);
        } catch (RuntimeException e) {
            log.warn("Committed shop write awaits cache recovery, event={}", id, e);
        }
    }

    public void execute(String id, Long shopId) {
        String token = UUID.randomUUID().toString();
        int claimed = updateState("UPDATE tb_cache_invalidation_task SET status='RUNNING',claim_token=?,"
                        + "lease_until=DATE_ADD(NOW(3),INTERVAL 30 SECOND),update_time=NOW(3) "
                        + "WHERE id=? AND business_type='SHOP' AND business_id=? AND status='PENDING' "
                        + "AND next_execute_time<=NOW(3)", token, id, shopId);
        if (claimed == 0) {
            List<Map<String, Object>> rows = queryState(
                    "SELECT status,business_id FROM tb_cache_invalidation_task WHERE id=?", id);
            if (rows.isEmpty()) throw new InvalidEvent("Missing cache task within retention window");
            if (((Number) rows.get(0).get("business_id")).longValue() != shopId) {
                throw new InvalidEvent("Cache task identity mismatch");
            }
            return;
        }
        try {
            cache.invalidateShop(shopId);
            local.invalidate(shopId);
        } catch (RuntimeException e) {
            int updated = updateState("UPDATE tb_cache_invalidation_task SET status='PENDING',"
                            + "next_execute_time=DATE_ADD(NOW(3),INTERVAL CASE retry_count WHEN 0 THEN 1 "
                            + "WHEN 1 THEN 5 WHEN 2 THEN 30 ELSE 60 END SECOND),"
                            + "next_dispatch_time=next_execute_time,retry_count=retry_count+1,"
                            + "claim_token=NULL,lease_until=NULL,dispatch_token=NULL,last_error=?,update_time=NOW(3) "
                            + "WHERE id=? AND status='RUNNING' AND claim_token=?",
                    error(e), id, token);
            if (updated == 0) log.warn("Cache task lease lost during failure, event={}", id);
            else log.warn("Cache invalidation deferred with durable backoff, event={}", id, e);
            return;
        }
        // Broker acknowledgement does not complete Redis work; only this token-guarded update does.
        updateState("UPDATE tb_cache_invalidation_task SET status='DONE',completed_time=NOW(3),"
                        + "update_time=NOW(3),claim_token=NULL,lease_until=NULL,dispatch_token=NULL,last_error=NULL "
                        + "WHERE id=? AND status='RUNNING' AND claim_token=?", id, token);
    }

    @Scheduled(fixedDelay = 1000)
    public void recoverAndDispatch() {
        try {
            jdbc.update("UPDATE tb_cache_invalidation_task SET status='PENDING',claim_token=NULL,lease_until=NULL,"
                    + "dispatch_token=NULL,next_execute_time=NOW(3),next_dispatch_time=NOW(3),update_time=NOW(3) "
                    + "WHERE status='RUNNING' AND lease_until<=NOW(3) ORDER BY lease_until LIMIT 100");
            List<Map<String, Object>> pending = jdbc.queryForList("SELECT id,business_id FROM tb_cache_invalidation_task "
                    + "WHERE status='PENDING' AND next_execute_time<=NOW(3) AND next_dispatch_time<=NOW(3) "
                    + "ORDER BY next_dispatch_time,id LIMIT 100");
            for (Map<String, Object> row : pending) submit(row, false);
            List<Map<String, Object>> broadcasts = jdbc.queryForList("SELECT id,business_id FROM tb_cache_invalidation_task "
                    + "WHERE status='DONE' AND broadcast_done=0 AND next_broadcast_time<=NOW(3) "
                    + "ORDER BY next_broadcast_time,id LIMIT 100");
            for (Map<String, Object> row : broadcasts) submit(row, true);
        } catch (RuntimeException e) { log.error("Cache outbox recovery unavailable", e); }
    }

    private void submit(Map<String, Object> row, boolean broadcast) {
        String id = row.get("id").toString();
        Long shopId = ((Number) row.get("business_id")).longValue();
        try {
            dispatch.execute(() -> {
                try { send(id, shopId, broadcast); }
                catch (Exception e) { log.warn("Cache outbox dispatch pending, event={}", id, e); }
            });
        } catch (RuntimeException e) { log.warn("Cache dispatch queue full; durable tasks retained"); }
    }

    private void send(String id, Long shopId, boolean broadcast) throws Exception {
        String token = UUID.randomUUID().toString();
        int reserved;
        if (broadcast) {
            reserved = jdbc.update("UPDATE tb_cache_invalidation_task SET broadcast_token=?,"
                            + "next_broadcast_time=DATE_ADD(NOW(3),INTERVAL 30 SECOND) "
                            + "WHERE id=? AND status='DONE' AND broadcast_done=0 AND next_broadcast_time<=NOW(3)", token, id);
        } else {
            reserved = jdbc.update("UPDATE tb_cache_invalidation_task SET dispatch_token=?,"
                            + "next_dispatch_time=DATE_ADD(NOW(3),INTERVAL 30 SECOND) WHERE id=? AND status='PENDING' "
                            + "AND next_execute_time<=NOW(3) AND next_dispatch_time<=NOW(3)", token, id);
        }
        if (reserved == 0) return;
        try {
            kafka.send(broadcast ? KafkaConfig.SHOP_CACHE_BROADCAST_TOPIC : KafkaConfig.SHOP_CACHE_INVALIDATION_TOPIC,
                    shopId.toString(), JSONUtil.toJsonStr(new Event(id, shopId))).get(11, TimeUnit.SECONDS);
            if (broadcast) {
                jdbc.update("UPDATE tb_cache_invalidation_task SET broadcast_done=1,broadcast_token=NULL,"
                                + "update_time=NOW(3) WHERE id=? AND status='DONE' AND broadcast_token=?", id, token);
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            if (broadcast) {
                jdbc.update("UPDATE tb_cache_invalidation_task SET broadcast_token=NULL,"
                                + "next_broadcast_time=DATE_ADD(NOW(3),INTERVAL 5 SECOND),last_error=? "
                                + "WHERE id=? AND broadcast_done=0 AND broadcast_token=?", error(e), id, token);
            } else {
                jdbc.update("UPDATE tb_cache_invalidation_task SET dispatch_token=NULL,dispatch_retry_count=dispatch_retry_count+1,"
                                + "next_dispatch_time=DATE_ADD(NOW(3),INTERVAL CASE dispatch_retry_count WHEN 1 THEN 1 "
                                + "WHEN 2 THEN 5 WHEN 3 THEN 30 ELSE 60 END SECOND),last_error=? "
                                + "WHERE id=? AND status='PENDING' AND dispatch_token=?", error(e), id, token);
            }
            throw e;
        }
    }

    public Event parse(String raw) {
        Event event;
        try {
            if (!JSONUtil.parseObj(raw).containsKey("version")) throw new InvalidEvent("Missing cache protocol version");
            event = JSONUtil.toBean(raw, Event.class);
        }
        catch (RuntimeException e) { throw new InvalidEvent("Malformed cache event"); }
        if (event == null || event.version != 1 || event.eventId == null ||
                event.eventId.length() != 36 || event.shopId == null || event.shopId <= 0) {
            throw new InvalidEvent("Unsupported cache event");
        }
        return event;
    }

    private static String error(Throwable e) {
        String message = e.toString();
        return message.substring(0, Math.min(500, message.length()));
    }

    private int updateState(String sql, Object... args) {
        return stateTransaction.execute(status -> jdbc.update(sql, args));
    }

    private List<Map<String, Object>> queryState(String sql, Object... args) {
        return stateTransaction.execute(status -> jdbc.queryForList(sql, args));
    }
}
