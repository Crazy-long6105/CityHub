package com.hmdp.utils.ratelimiter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class TrustedClientIpResolver {
    private final List<Network> trustedNetworks = new ArrayList<>();

    public TrustedClientIpResolver(RateLimiterProperties properties) {
        for (String cidr : properties.getTrustedProxyCidrs()) {
            trustedNetworks.add(new Network(cidr));
        }
        if (trustedNetworks.isEmpty()) {
            log.info("Rate limiter has no trusted proxies; forwarded IP headers are ignored");
        }
    }

    public String resolve(HttpServletRequest request) {
        InetAddress peer = literalAddress(request.getRemoteAddr());
        String forwarded = request.getHeader("X-Forwarded-For");
        if (!isTrusted(peer) || forwarded == null || forwarded.length() > 2048) {
            return peer.getHostAddress();
        }
        String[] chain = forwarded.split(",", -1);
        if (chain.length > 32) {
            return peer.getHostAddress();
        }
        InetAddress candidate = peer;
        try {
            for (int i = chain.length - 1; i >= 0 && isTrusted(candidate); i--) {
                candidate = literalAddress(chain[i].trim());
            }
            return candidate.getHostAddress();
        } catch (IllegalArgumentException ex) {
            return peer.getHostAddress();
        }
    }

    private boolean isTrusted(InetAddress address) {
        for (Network network : trustedNetworks) {
            if (network.contains(address)) {
                return true;
            }
        }
        return false;
    }

    private static InetAddress literalAddress(String value) {
        if (value == null || value.length() > 45 || !value.matches("[0-9a-fA-F:.]+")) {
            throw new IllegalArgumentException("Expected an IP address literal");
        }
        try {
            if (value.indexOf(':') >= 0) {
                return InetAddress.getByName(value);
            }
            String[] parts = value.split("\\.", -1);
            if (parts.length != 4) {
                throw new IllegalArgumentException("Invalid IPv4 address");
            }
            byte[] bytes = new byte[4];
            for (int i = 0; i < 4; i++) {
                if (!parts[i].matches("0|[1-9][0-9]{0,2}") || Integer.parseInt(parts[i]) > 255) {
                    throw new IllegalArgumentException("Invalid IPv4 address");
                }
                bytes[i] = (byte) Integer.parseInt(parts[i]);
            }
            return InetAddress.getByAddress(bytes);
        } catch (UnknownHostException ex) {
            throw new IllegalArgumentException("Invalid IP address", ex);
        }
    }

    private static final class Network {
        private final byte[] address;
        private final int prefix;

        private Network(String cidr) {
            String[] parts = cidr.split("/", -1);
            if (parts.length > 2) {
                throw new IllegalArgumentException("Invalid trusted proxy CIDR");
            }
            address = literalAddress(parts[0]).getAddress();
            prefix = parts.length == 1 ? address.length * 8 : Integer.parseInt(parts[1]);
            if (prefix < 0 || prefix > address.length * 8) {
                throw new IllegalArgumentException("Invalid trusted proxy prefix");
            }
        }

        private boolean contains(InetAddress candidate) {
            byte[] bytes = candidate.getAddress();
            if (bytes.length != address.length) {
                return false;
            }
            for (int i = 0; i < prefix; i++) {
                int mask = 1 << (7 - i % 8);
                if ((bytes[i / 8] & mask) != (address[i / 8] & mask)) {
                    return false;
                }
            }
            return true;
        }
    }
}
