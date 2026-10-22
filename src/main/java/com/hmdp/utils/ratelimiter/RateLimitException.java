package com.hmdp.utils.ratelimiter;

public class RateLimitException extends RuntimeException {
    private final Reason reason;

    public RateLimitException(String message) {
        this(message, Reason.REJECTED);
    }

    public RateLimitException(String message, Reason reason) {
        super(message);
        this.reason = reason;
    }

    public Reason getReason() {
        return reason;
    }

    public enum Reason {
        REJECTED, DEPENDENCY_UNAVAILABLE, UNAUTHENTICATED
    }
}
