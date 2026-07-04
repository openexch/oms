// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.rest;

import com.openexchange.oms.api.OrderService;
import com.openexchange.oms.api.audit.AuditLog;
import com.openexchange.oms.api.auth.ApiKeyAuthenticationProvider;
import com.openexchange.oms.api.auth.HttpAuthHandler;
import com.openexchange.oms.api.auth.RoleBasedAuthorizer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** oms#38: /metrics serves a Prometheus scrape without credentials. */
class MetricsEndpointTest {

    // Secure provider with no matching key: proves /metrics bypasses auth.
    private static final ApiKeyAuthenticationProvider PROVIDER =
            ApiKeyAuthenticationProvider.parse("some-key:1", "");

    private FullHttpResponse get(String uri, PrometheusMeterRegistry registry) {
        OrderService orderService = mock(OrderService.class);
        when(orderService.isClusterConnected()).thenReturn(true);
        EmbeddedChannel ch = new EmbeddedChannel(
                new HttpAuthHandler(PROVIDER),
                new RestApiHandler(orderService, null, new RoleBasedAuthorizer(),
                        CorsPolicy.fromSpec(""), AuditLog.disabled(), registry));
        ch.writeInbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri, Unpooled.EMPTY_BUFFER));
        FullHttpResponse resp = ch.readOutbound();
        assertNotNull(resp);
        return resp;
    }

    @Test
    void metricsScrapeWithoutCredentials() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        registry.counter("oms_test_counter_total").increment();

        FullHttpResponse resp = get("/metrics", registry);
        assertEquals(HttpResponseStatus.OK, resp.status());
        String body = resp.content().toString(StandardCharsets.UTF_8);
        assertTrue(body.contains("oms_test_counter_total"), "scrape output expected");
        assertTrue(resp.headers().get("Content-Type").startsWith("text/plain"));
        resp.release();
    }

    @Test
    void requestTimerRecordsRouteLabel() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        get("/api/v1/health", registry).release();

        FullHttpResponse resp = get("/metrics", registry);
        String body = resp.content().toString(StandardCharsets.UTF_8);
        assertTrue(body.contains("oms_http_request_seconds") && body.contains("route=\"health\""),
                "request timer with route label expected, got:\n" + body.substring(0, Math.min(500, body.length())));
        resp.release();
    }
}
