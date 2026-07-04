// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.rest;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CORS allowlist (oms#37). The default is NO allowed origins — no CORS
 * headers are emitted and browsers refuse cross-origin reads (same-origin
 * setups, e.g. behind the reverse proxy or the vite dev proxy, are
 * unaffected). Configure OMS_CORS_ORIGINS with a comma-separated origin
 * list, or "*" to allow any origin (dev only).
 */
public final class CorsPolicy {

    private final Set<String> allowedOrigins;
    private final boolean allowAll;

    private CorsPolicy(Set<String> allowedOrigins, boolean allowAll) {
        this.allowedOrigins = allowedOrigins;
        this.allowAll = allowAll;
    }

    public static CorsPolicy fromSpec(String spec) {
        if (spec == null || spec.isBlank()) {
            return new CorsPolicy(Set.of(), false);
        }
        Set<String> origins = Arrays.stream(spec.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return new CorsPolicy(origins, origins.contains("*"));
    }

    /** Add CORS headers iff the request Origin is allowlisted. */
    public void apply(String requestOrigin, FullHttpResponse response) {
        if (requestOrigin == null) return;
        String allowed = allowAll ? "*" : (allowedOrigins.contains(requestOrigin) ? requestOrigin : null);
        if (allowed == null) return;
        response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, allowed);
        response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, PUT, DELETE, OPTIONS");
        response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type, Authorization, X-API-Key");
        response.headers().set("Access-Control-Allow-Private-Network", "true");
        if (!allowAll) {
            response.headers().set(HttpHeaderNames.VARY, "Origin");
        }
    }
}
