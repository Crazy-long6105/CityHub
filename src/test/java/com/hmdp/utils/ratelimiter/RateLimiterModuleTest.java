package com.hmdp.utils.ratelimiter;

import com.hmdp.dto.UserDTO;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hmdp.utils.ratelimiter.RateLimiter.LimitType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RateLimiterModuleTest {
    @AfterEach
    void clearContext() {
        UserHolder.removeUser();
        RequestContextHolder.resetRequestAttributes();
    }

    @Test
    void configurationCompatibilityAndUnsafeDefinitionsFailAtStartup() throws Exception {
        assertEquals(Collections.singleton(IP), RateLimiterConfigurationValidator.dimensions(annotation("legacy")));
        assertEquals(3, RateLimiterConfigurationValidator.dimensions(annotation("limited")).size());
        RateLimiterConfigurationValidator validator = new RateLimiterConfigurationValidator();
        assertThrows(IllegalArgumentException.class, () -> validator.postProcessAfterInitialization(new Conflict(), "conflict"));
        assertThrows(IllegalArgumentException.class, () -> validator.postProcessAfterInitialization(new Unsafe(), "unsafe"));
    }

    @Test
    void forwardedAddressesRequireTrustedPeerAndStopAtFirstUntrustedHop() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("198.51.100.7");
        request.addHeader("X-Forwarded-For", "192.0.2.1");
        assertEquals("198.51.100.7", new TrustedClientIpResolver(new RateLimiterProperties()).resolve(request));
        RateLimiterProperties properties = new RateLimiterProperties();
        properties.setTrustedProxyCidrs(Collections.singletonList("10.0.0.0/8"));
        TrustedClientIpResolver resolver = new TrustedClientIpResolver(properties);
        request.setRemoteAddr("10.0.0.2");
        request.removeHeader("X-Forwarded-For");
        request.addHeader("X-Forwarded-For", "192.0.2.99, 198.51.100.7, 10.0.0.1");
        assertEquals("198.51.100.7", resolver.resolve(request));
        request.removeHeader("X-Forwarded-For");
        request.addHeader("X-Forwarded-For", "malformed");
        assertEquals("10.0.0.2", resolver.resolve(request));
    }

    @Test
    @SuppressWarnings("unchecked")
    void oneAtomicCallCarriesAllDimensionsAndDenialNeverEntersBusiness() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        List<List<String>> seenKeys = new ArrayList<>();
        List<String> members = new ArrayList<>();
        doAnswer(call -> {
            seenKeys.add(new ArrayList<>((List<String>) call.getArgument(1)));
            members.add((String) call.getArgument(4));
            return seenKeys.size() == 1 ? Arrays.asList(1L, 0L, 1L, 1L, 1L) : Arrays.asList(0L, 3L, 1L, 1L, 10L);
        }).when(redis).execute(any(RedisScript.class), anyList(), any(), any(), any());
        RateLimiterAspect aspect = aspect(redis);
        Endpoint target = new Endpoint();
        Endpoint proxy = proxy(target, aspect);
        context();
        proxy.limited();
        RateLimitException error = assertThrows(RateLimitException.class, proxy::limited);
        assertEquals(RateLimitException.Reason.REJECTED, error.getReason());
        assertEquals(1, target.calls.get());
        assertEquals(3, seenKeys.get(0).size());
        assertEquals(seenKeys.get(0), seenKeys.get(1));
        assertTrue(seenKeys.get(0).stream().anyMatch(key -> key.endsWith(":user:42")));
        assertTrue(seenKeys.get(0).stream().anyMatch(key -> key.endsWith(":ip:198.51.100.7")));
        assertNotEquals(members.get(0), members.get(1));
        assertEquals(1, aspect.getAllowedCount());
        assertEquals(1, aspect.getRejectedCount());
    }

    @Test
    @SuppressWarnings("unchecked")
    void redisNullErrorsAndUnauthenticatedUsersFailClosedButExplicitOpenCanProceed() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        RateLimiterAspect aspect = aspect(redis);
        Endpoint target = new Endpoint();
        Endpoint proxy = proxy(target, aspect);
        RateLimitException unauthenticated = assertThrows(RateLimitException.class, proxy::limited);
        assertEquals(RateLimitException.Reason.UNAUTHENTICATED, unauthenticated.getReason());
        verifyNoInteractions(redis);
        context();
        RateLimitException unavailable = assertThrows(RateLimitException.class, proxy::limited);
        assertEquals(RateLimitException.Reason.DEPENDENCY_UNAVAILABLE, unavailable.getReason());
        doThrow(new IllegalStateException("offline")).when(redis).execute(any(RedisScript.class), anyList(), any(), any(), any());
        assertThrows(RateLimitException.class, proxy::limited);
        proxy.open();
        assertEquals(1, target.calls.get());
        assertEquals(3, aspect.getDependencyErrorCount());
    }

    @Test
    void realRedisAtomicQuotaConcurrencyAndExpiry() throws Exception {
        String host = System.getenv("REDIS_TEST_HOST");
        assumeTrue(host != null && !host.isEmpty(), "Set REDIS_TEST_HOST to run real Redis checks");
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(host,
                Integer.parseInt(System.getenv().getOrDefault("REDIS_TEST_PORT", "6379")));
        String password = System.getenv("REDIS_TEST_PASSWORD");
        if (password != null && !password.isEmpty()) {
            config.setPassword(password);
        }
        LettuceConnectionFactory factory = new LettuceConnectionFactory(config);
        factory.afterPropertiesSet();
        StringRedisTemplate first = new StringRedisTemplate(factory);
        StringRedisTemplate second = new StringRedisTemplate(factory);
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("limiter.lua"));
        script.setResultType(List.class);
        String prefix = "test:limiter:{" + UUID.randomUUID() + "}:";
        List<String> keys = Arrays.asList(prefix + "method", prefix + "ip", prefix + "user");
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Callable<Boolean>> calls = new ArrayList<>();
            for (int i = 0; i < 80; i++) {
                StringRedisTemplate client = i % 2 == 0 ? first : second;
                calls.add(() -> ((Long) client.execute(script, keys, "10", "17", UUID.randomUUID().toString()).get(0)) == 1L);
            }
            long allowed = 0;
            for (Future<Boolean> future : pool.invokeAll(calls)) {
                if (future.get()) { allowed++; }
            }
            assertEquals(17, allowed);
            for (String key : keys) {
                assertEquals(Long.valueOf(17), first.opsForZSet().size(key));
                assertTrue(first.getExpire(key) > 0);
            }
            first.delete(keys);
            first.opsForZSet().add(keys.get(1), "old", Double.MAX_VALUE);
            List<?> denied = first.execute(script, keys, "1", "1", UUID.randomUUID().toString());
            assertEquals(0L, denied.get(0));
            assertFalse(first.hasKey(keys.get(0)));
            assertFalse(first.hasKey(keys.get(2)));
            first.delete(keys);
            first.opsForValue().set(keys.get(2), "wrong-type");
            assertThrows(RuntimeException.class, () -> first.execute(script, keys, "1", "1", UUID.randomUUID().toString()));
            assertFalse(first.hasKey(keys.get(0)));
            first.delete(keys);
            first.opsForZSet().add(keys.get(0), "expired", 1);
            assertEquals(1L, first.execute(script, keys, "1", "1", UUID.randomUUID().toString()).get(0));
            Thread.sleep(1150);
            assertEquals(Long.valueOf(0), first.countExistingKeys(keys));
        } finally {
            pool.shutdownNow();
            first.delete(keys);
            factory.destroy();
        }
    }

    private static RateLimiter annotation(String name) throws NoSuchMethodException {
        return Endpoint.class.getDeclaredMethod(name).getAnnotation(RateLimiter.class);
    }

    private static RateLimiterAspect aspect(StringRedisTemplate redis) {
        return new RateLimiterAspect(redis, new TrustedClientIpResolver(new RateLimiterProperties()));
    }

    private static Endpoint proxy(Endpoint target, RateLimiterAspect aspect) {
        AspectJProxyFactory factory = new AspectJProxyFactory(target);
        factory.addAspect(aspect);
        return factory.getProxy();
    }

    private static void context() {
        UserDTO user = new UserDTO();
        user.setId(42L);
        UserHolder.saveUser(user);
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr("198.51.100.7");
        request.addHeader("X-Forwarded-For", "192.0.2.99");
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
    }

    static class Endpoint {
        final AtomicInteger calls = new AtomicInteger();
        @RateLimiter(dimensions = {METHOD, IP, USER}, limit = 10, highRisk = true, requireLogin = true)
        public void limited() { calls.incrementAndGet(); }
        @RateLimiter(type = IP)
        public void legacy() { }
        @RateLimiter(failurePolicy = RateLimiter.FailurePolicy.OPEN)
        public void open() { calls.incrementAndGet(); }
    }

    static class Conflict {
        @RateLimiter(type = IP, dimensions = {USER})
        public void conflict() { }
    }

    static class Unsafe {
        @RateLimiter(highRisk = true, failurePolicy = RateLimiter.FailurePolicy.OPEN)
        public void unsafe() { }
    }
}
