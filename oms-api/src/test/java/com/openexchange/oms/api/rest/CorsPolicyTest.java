// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.rest;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CorsPolicyTest {

    private static FullHttpResponse resp() {
        return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    }

    @Test
    void defaultEmitsNoCorsHeaders() {
        FullHttpResponse response = resp();
        CorsPolicy.fromSpec("").apply("https://evil.example", response);
        assertNull(response.headers().get("Access-Control-Allow-Origin"));
    }

    @Test
    void allowlistedOriginIsEchoed() {
        CorsPolicy policy = CorsPolicy.fromSpec("https://trade.example.com, https://ops.example.com");
        FullHttpResponse allowed = resp();
        policy.apply("https://trade.example.com", allowed);
        assertEquals("https://trade.example.com", allowed.headers().get("Access-Control-Allow-Origin"));
        assertEquals("Origin", allowed.headers().get("Vary"));
        assertTrue(allowed.headers().get("Access-Control-Allow-Headers").contains("Authorization"));

        FullHttpResponse denied = resp();
        policy.apply("https://evil.example", denied);
        assertNull(denied.headers().get("Access-Control-Allow-Origin"));
    }

    @Test
    void wildcardAllowsAll() {
        FullHttpResponse response = resp();
        CorsPolicy.fromSpec("*").apply("https://anything.example", response);
        assertEquals("*", response.headers().get("Access-Control-Allow-Origin"));
    }

    @Test
    void noOriginHeaderMeansNoCorsHeaders() {
        FullHttpResponse response = resp();
        CorsPolicy.fromSpec("*").apply(null, response);
        assertNull(response.headers().get("Access-Control-Allow-Origin"));
    }
}
