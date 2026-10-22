package com.hmdp.utils.ratelimiter;

import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

import java.util.EnumSet;

@Component
public class RateLimiterConfigurationValidator implements BeanPostProcessor {
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        ReflectionUtils.doWithMethods(AopUtils.getTargetClass(bean), method -> {
            RateLimiter limiter = AnnotationUtils.findAnnotation(method, RateLimiter.class);
            if (limiter != null) {
                try {
                    dimensions(limiter);
                } catch (IllegalArgumentException ex) {
                    throw new IllegalArgumentException("Invalid @RateLimiter on " + method.toGenericString(), ex);
                }
            }
        });
        return bean;
    }

    static EnumSet<RateLimiter.LimitType> dimensions(RateLimiter limiter) {
        if (limiter.window() < 1 || limiter.window() > 86400 || limiter.limit() < 1
                || limiter.limit() > 1000000 || !limiter.key().matches("[a-zA-Z0-9:_-]{1,64}")) {
            throw new IllegalArgumentException("Invalid window, limit or key prefix");
        }
        if (limiter.highRisk() && limiter.failurePolicy() == RateLimiter.FailurePolicy.OPEN) {
            throw new IllegalArgumentException("High-risk endpoints must fail closed");
        }
        EnumSet<RateLimiter.LimitType> dimensions = EnumSet.noneOf(RateLimiter.LimitType.class);
        for (RateLimiter.LimitType dimension : limiter.dimensions()) {
            if (!dimensions.add(dimension)) {
                throw new IllegalArgumentException("Duplicate rate-limit dimension");
            }
        }
        // METHOD is also the annotation default and means unspecified with dimensions.
        if (!dimensions.isEmpty() && limiter.type() != RateLimiter.LimitType.METHOD
                && (dimensions.size() != 1 || !dimensions.contains(limiter.type()))) {
            throw new IllegalArgumentException("Conflicting type and dimensions");
        }
        if (dimensions.isEmpty()) {
            dimensions.add(limiter.type());
        }
        return dimensions;
    }
}
