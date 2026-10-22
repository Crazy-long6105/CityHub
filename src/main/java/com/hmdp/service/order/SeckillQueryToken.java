package com.hmdp.service.order;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

@Component
public class SeckillQueryToken {
    private final byte[] secret;
    public SeckillQueryToken(@Value("${hmdp.seckill.query-secret:}") String secret) {
        this.secret = secret.getBytes(StandardCharsets.UTF_8);
    }
    public String issue(Long order, Long user, long permit, long expiry) {
        String payload = order + ":" + user + ":" + permit + ":" + expiry;
        return payload + ":" + Base64.getUrlEncoder().withoutPadding().encodeToString(sign(payload));
    }
    public long[] verify(String token, Long order, Long user) {
        if (token == null || token.length() > 256) throw new IllegalArgumentException("Invalid query credential");
        String[] parts = token.split(":", -1);
        if (parts.length != 5 || !order.toString().equals(parts[0]) || !user.toString().equals(parts[1]))
            throw new IllegalArgumentException("Invalid query credential");
        String payload = parts[0] + ":" + parts[1] + ":" + parts[2] + ":" + parts[3];
        if (!MessageDigest.isEqual(sign(payload), Base64.getUrlDecoder().decode(parts[4])))
            throw new IllegalArgumentException("Invalid query credential");
        return new long[] {Long.parseLong(parts[2]), Long.parseLong(parts[3])};
    }
    private byte[] sign(String payload) {
        if (secret.length < 32) throw new IllegalStateException("Configure a query secret of at least 32 bytes");
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            return mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
        } catch (java.security.GeneralSecurityException e) { throw new IllegalStateException(e); }
    }
}
