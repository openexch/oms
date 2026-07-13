// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.rest;

import com.openexchange.oms.api.AdminService;
import com.openexchange.oms.api.OrderService;
import com.openexchange.oms.api.auth.ApiKeyAuthenticationProvider;
import com.openexchange.oms.api.auth.HttpAuthHandler;
import com.openexchange.oms.api.auth.RoleBasedAuthorizer;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Admin risk endpoints with the persist hop: the in-memory apply happens on the event
 * loop, the JDBC persist on the oms-risk-persist executor, and the response (written
 * from that executor) reports durability via "persisted". A store failure is still a
 * 200 with persisted:false, never a 5xx.
 */
class RestApiHandlerRiskAdminTest {

    private static final ApiKeyAuthenticationProvider PROVIDER =
            ApiKeyAuthenticationProvider.parse("admin-key:2:ADMIN", "");

    private AdminService adminService;

    @BeforeEach
    void setUp() {
        adminService = mock(AdminService.class);
    }

    private EmbeddedChannel channel() {
        return new EmbeddedChannel(
                new HttpAuthHandler(PROVIDER),
                new RestApiHandler(mock(OrderService.class), adminService, new RoleBasedAuthorizer(),
                        CorsPolicy.fromSpec(""), com.openexchange.oms.api.audit.AuditLog.disabled(),
                        new io.micrometer.prometheusmetrics.PrometheusMeterRegistry(
                                io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT)));
    }

    private static FullHttpRequest request(HttpMethod method, String uri, String body) {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri,
                body == null ? Unpooled.EMPTY_BUFFER
                        : Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
        req.headers().set("X-API-Key", "admin-key");
        return req;
    }

    /** The response is written from the persist executor thread; poll for it. */
    private static FullHttpResponse awaitResponse(EmbeddedChannel channel) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            channel.runPendingTasks();
            FullHttpResponse response = channel.readOutbound();
            if (response != null) {
                return response;
            }
            Thread.sleep(5);
        }
        fail("no response within 5s");
        return null;
    }

    private static String bodyOf(FullHttpResponse response) {
        return response.content().toString(StandardCharsets.UTF_8);
    }

    @Test
    void updateRiskConfig_persistSucceeds_persistedTrue() throws Exception {
        when(adminService.persistRiskConfig(1)).thenReturn(true);

        EmbeddedChannel ch = channel();
        ch.writeInbound(request(HttpMethod.PUT, "/api/v1/admin/risk/config/1", "{\"maxOpenOrders\":42}"));
        FullHttpResponse response = awaitResponse(ch);

        assertEquals(HttpResponseStatus.OK, response.status());
        String body = bodyOf(response);
        assertTrue(body.contains("\"success\":true"));
        assertTrue(body.contains("\"persisted\":true"));

        // Apply-then-persist ordering
        InOrder inOrder = inOrder(adminService);
        inOrder.verify(adminService).updateRiskConfig(eq(1), any());
        inOrder.verify(adminService).persistRiskConfig(1);
    }

    @Test
    void updateRiskConfig_persistFails_still200PersistedFalse() throws Exception {
        when(adminService.persistRiskConfig(1)).thenReturn(false);

        EmbeddedChannel ch = channel();
        ch.writeInbound(request(HttpMethod.PUT, "/api/v1/admin/risk/config/1", "{\"maxOpenOrders\":42}"));
        FullHttpResponse response = awaitResponse(ch);

        assertEquals(HttpResponseStatus.OK, response.status());
        String body = bodyOf(response);
        assertTrue(body.contains("\"success\":true"));
        assertTrue(body.contains("\"persisted\":false"));
        verify(adminService).updateRiskConfig(eq(1), any()); // in-memory apply still happened
    }

    @Test
    void circuitBreakerTrip_persistsManualTripTrue() throws Exception {
        when(adminService.persistManualTrip(1, true)).thenReturn(true);

        EmbeddedChannel ch = channel();
        ch.writeInbound(request(HttpMethod.POST, "/api/v1/admin/risk/circuit-breaker/1/trip", null));
        FullHttpResponse response = awaitResponse(ch);

        assertEquals(HttpResponseStatus.OK, response.status());
        String body = bodyOf(response);
        assertTrue(body.contains("\"action\":\"trip\""));
        assertTrue(body.contains("\"persisted\":true"));

        InOrder inOrder = inOrder(adminService);
        inOrder.verify(adminService).tripCircuitBreaker(1);
        inOrder.verify(adminService).persistManualTrip(1, true);
    }

    @Test
    void circuitBreakerReset_persistsManualTripFalse() throws Exception {
        when(adminService.persistManualTrip(1, false)).thenReturn(true);

        EmbeddedChannel ch = channel();
        ch.writeInbound(request(HttpMethod.POST, "/api/v1/admin/risk/circuit-breaker/1/reset", null));
        FullHttpResponse response = awaitResponse(ch);

        assertEquals(HttpResponseStatus.OK, response.status());
        assertTrue(bodyOf(response).contains("\"persisted\":true"));

        InOrder inOrder = inOrder(adminService);
        inOrder.verify(adminService).resetCircuitBreaker(1);
        inOrder.verify(adminService).persistManualTrip(1, false);
    }

    @Test
    void circuitBreakerUnknownAction_400_neverPersists() throws Exception {
        EmbeddedChannel ch = channel();
        ch.writeInbound(request(HttpMethod.POST, "/api/v1/admin/risk/circuit-breaker/1/explode", null));
        ch.runPendingTasks();
        FullHttpResponse response = ch.readOutbound(); // synchronous error path
        assertNotNull(response);
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        verify(adminService, never()).persistManualTrip(anyInt(), anyBoolean());
    }
}
