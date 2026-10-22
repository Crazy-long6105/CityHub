package com.hmdp.utils.ratelimiter;

import com.hmdp.dto.UserDTO;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.util.DigestUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;

@Slf4j
@Aspect
@Order(0)
@Component
public class RateLimiterAspect {
    private static final DefaultRedisScript<List> SCRIPT = new DefaultRedisScript<>();
    static {
        SCRIPT.setLocation(new ClassPathResource("limiter.lua"));
        SCRIPT.setResultType(List.class);
    }

    private final StringRedisTemplate redis;
    private final TrustedClientIpResolver ipResolver;
    private final LongAdder allowed = new LongAdder();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder dependencyErrors = new LongAdder();
    private final LongAdder redisNanos = new LongAdder();

    public RateLimiterAspect(StringRedisTemplate redis, TrustedClientIpResolver ipResolver) {
        this.redis = redis;
        this.ipResolver = ipResolver;
    }

    @Before("@annotation(limiter)")
    public void doBefore(JoinPoint point, RateLimiter limiter) {
        EnumSet<RateLimiter.LimitType> dimensions = RateLimiterConfigurationValidator.dimensions(limiter);
        UserDTO user = UserHolder.getUser();
        if (limiter.requireLogin() && (user == null || user.getId() == null)) {
            throw new RateLimitException("请先登录", RateLimitException.Reason.UNAUTHENTICATED);
        }
        Method method = ((MethodSignature) point.getSignature()).getMethod();
        method = AopUtils.getMostSpecificMethod(method, point.getTarget().getClass());
        String methodName = method.getDeclaringClass().getName() + "." + method.getName();
        long start = System.nanoTime();
        List<?> result;
        try {
            List<String> keys = buildKeys(method, limiter, dimensions, user);
            result = redis.execute(SCRIPT, keys, Integer.toString(limiter.window()),
                    Integer.toString(limiter.limit()), UUID.randomUUID().toString());
            validateResult(result, dimensions.size(), limiter.limit());
        } catch (RuntimeException ex) {
            dependencyErrors.increment();
            long failures = dependencyErrors.sum();
            if (sample(failures)) {
                log.warn("rate_limit method={} version=1 outcome=dependency_error failures={} cause={}",
                        methodName, failures, ex.getClass().getSimpleName());
            }
            if (limiter.highRisk() || limiter.failurePolicy() == RateLimiter.FailurePolicy.CLOSED) {
                throw new RateLimitException("系统繁忙，请稍后再试", RateLimitException.Reason.DEPENDENCY_UNAVAILABLE);
            }
            return;
        } finally {
            redisNanos.add(System.nanoTime() - start);
        }
        if (((Number) result.get(0)).longValue() == 0) {
            rejected.increment();
            long rejections = rejected.sum();
            if (sample(rejections)) {
                int index = ((Number) result.get(1)).intValue() - 1;
                log.warn("rate_limit method={} version=1 outcome=rejected dimension={} window={} limit={} rejected={}",
                        methodName, new ArrayList<>(dimensions).get(index), limiter.window(), limiter.limit(), rejections);
            }
            throw new RateLimitException(limiter.message());
        }
        allowed.increment();
        log.debug("rate_limit method={} version=1 outcome=allowed dimensions={} window={} limit={}",
                methodName, dimensions, limiter.window(), limiter.limit());
    }

    private List<String> buildKeys(Method method, RateLimiter limiter,
                                   EnumSet<RateLimiter.LimitType> dimensions, UserDTO user) {
        String methodHash = DigestUtils.md5DigestAsHex(method.toGenericString().getBytes(StandardCharsets.UTF_8));
        String prefix = limiter.key() + "{" + methodHash + "}";
        List<String> keys = new ArrayList<>();
        for (RateLimiter.LimitType dimension : dimensions) {
            switch (dimension) {
                case IP:
                    ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
                    if (attributes == null) {
                        throw new IllegalStateException("No HTTP request for IP rate limit");
                    }
                    keys.add(prefix + ":ip:" + ipResolver.resolve(attributes.getRequest()));
                    break;
                case USER:
                    keys.add(prefix + ":user:" + (user == null || user.getId() == null ? "anonymous" : user.getId()));
                    break;
                default:
                    keys.add(prefix + ":method");
            }
        }
        return keys;
    }

    private static void validateResult(List<?> result, int dimensions, int limit) {
        if (result == null || result.size() != dimensions + 2) {
            throw new IllegalStateException("Unknown rate limiter result");
        }
        for (Object value : result) {
            if (!(value instanceof Long)) {
                throw new IllegalStateException("Invalid rate limiter result type");
            }
        }
        long status = (Long) result.get(0);
        long index = (Long) result.get(1);
        if ((status != 0 && status != 1) || (status == 1 && index != 0)
                || (status == 0 && (index < 1 || index > dimensions))) {
            throw new IllegalStateException("Invalid rate limiter status");
        }
        for (int i = 2; i < result.size(); i++) {
            long count = (Long) result.get(i);
            if (count < 0 || (status == 1 && (count < 1 || count > limit))) {
                throw new IllegalStateException("Invalid rate limiter count");
            }
        }
    }

    private static boolean sample(long count) {
        return (count & (count - 1)) == 0 || count % 1024 == 0;
    }

    public long getAllowedCount() { return allowed.sum(); }
    public long getRejectedCount() { return rejected.sum(); }
    public long getDependencyErrorCount() { return dependencyErrors.sum(); }
    public long getRedisNanos() { return redisNanos.sum(); }
}
