package com.hmdp.service.order;

import com.hmdp.dto.SeckillMessage;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;

@Component
public class SeckillRedisStore {
    public static final String ROOT = "seckill:v2:";
    public static final String PENDING = ROOT + "pending";
    private final StringRedisTemplate redis;
    private static final DefaultRedisScript<List> RESERVE = script("seckill_advance.lua", List.class);
    private static final DefaultRedisScript<List> READ = script("seckill_read.lua", List.class);
    private static final DefaultRedisScript<List> CLAIM = script("seckill_claim.lua", List.class);
    private static final DefaultRedisScript<Long> TRANSITION = script("seckill_transition.lua", Long.class);
    private static final DefaultRedisScript<Long> RELEASE = script("seckill_release.lua", Long.class);
    private static final DefaultRedisScript<Long> INITIALIZE = script("seckill_initialize.lua", Long.class);

    public SeckillRedisStore(StringRedisTemplate redis) { this.redis = redis; }

    private static <T> DefaultRedisScript<T> script(String name, Class<T> type) {
        DefaultRedisScript<T> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(name));
        script.setResultType(type);
        return script;
    }

    public List<String> reserve(Long voucher, Long user, Long order, long permit, long expiry) {
        List result = redis.execute(RESERVE, Arrays.asList(activity(voucher), stock(voucher),
                qualification(voucher), request(order), PENDING), voucher.toString(), user.toString(),
                order.toString(), String.valueOf(permit), String.valueOf(expiry));
        if (result == null || result.size() != 2) throw new IllegalStateException("Reservation result unknown");
        return Arrays.asList(result.get(0).toString(), result.get(1).toString());
    }

    public Snapshot read(Long order) {
        List result = redis.execute(READ, Collections.singletonList(request(order)));
        if (result == null || result.isEmpty()) throw new IllegalStateException("Request result unavailable");
        Map<String, String> fields = new HashMap<>();
        for (int i = 1; i + 1 < result.size(); i += 2) fields.put(result.get(i).toString(), result.get(i + 1).toString());
        return new Snapshot(Long.parseLong(result.get(0).toString()), fields);
    }

    public void initialize(Long voucher, int count, long begin, long end) {
        Long result = redis.execute(INITIALIZE, Arrays.asList(activity(voucher), stock(voucher), qualification(voucher)),
                String.valueOf(count), String.valueOf(begin), String.valueOf(end));
        if (!Long.valueOf(1).equals(result)) throw new IllegalStateException("Activity initialization failed");
    }

    public boolean beginSend(Long order) { return Long.valueOf(1).equals(transition(order, "SEND", "", "")); }
    public void acknowledgeSend(Long order) { transition(order, "ACK", "", ""); }
    public void retryLater(Long order, long delay) { transition(order, "WAIT", String.valueOf(delay), ""); }
    public void finish(Long order, boolean success, String reason) {
        transition(order, "FINISH", success ? "SUCCESS" : "FAILED", reason == null ? "" : reason);
    }

    private Long transition(Long order, String operation, String value, String reason) {
        Long result = redis.execute(TRANSITION, Arrays.asList(request(order), PENDING),
                operation, value, reason, order.toString());
        if (result == null) throw new IllegalStateException("Request transition unknown");
        return result;
    }

    public List<Long> claim(int limit) {
        List result = redis.execute(CLAIM, Collections.singletonList(PENDING), String.valueOf(Math.min(limit, 100)));
        if (result == null) throw new IllegalStateException("Recovery index unavailable");
        List<Long> ids = new ArrayList<>();
        for (Object value : result) ids.add(Long.valueOf(value.toString()));
        return ids;
    }

    public void release(Long orderId, Long userId, Long voucherId) {
        Long result = redis.execute(RELEASE, Arrays.asList(stock(voucherId), qualification(voucherId), marker(orderId)),
                userId.toString(), orderId.toString());
        if (result == null || result < 0 || result > 2) throw new IllegalStateException("Release result unknown");
    }

    public void expireReleaseMarker(Long orderId) { redis.expire(marker(orderId), Duration.ofHours(24)); }
    private static String activity(Long id) { return ROOT + "activity:" + id; }
    private static String stock(Long id) { return ROOT + "stock:" + id; }
    private static String qualification(Long id) { return ROOT + "qualification:" + id; }
    private static String request(Long id) { return ROOT + "request:" + id; }
    private static String marker(Long id) { return ROOT + "released:" + id; }

    public static class Snapshot {
        public final long now;
        public final Map<String, String> fields;
        public Snapshot(long now, Map<String, String> fields) { this.now = now; this.fields = fields; }
        public boolean missing() { return fields.isEmpty(); }
        public boolean terminal() { return "SUCCESS".equals(fields.get("status")) || "FAILED".equals(fields.get("status")); }
        public SeckillMessage message() {
            SeckillMessage message = new SeckillMessage(Long.valueOf(fields.get("userId")),
                    Long.valueOf(fields.get("voucherId")), Long.valueOf(fields.get("orderId")));
            message.setCreatedAt(Long.valueOf(fields.get("createdAt")));
            message.setDeadline(Long.valueOf(fields.get("deadline")));
            return message;
        }
    }
}
