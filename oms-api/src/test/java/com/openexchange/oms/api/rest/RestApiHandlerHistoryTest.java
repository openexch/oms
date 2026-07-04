package com.openexchange.oms.api.rest;

import com.openexchange.oms.api.AdminService;
import com.openexchange.oms.api.OrderService;
import com.openexchange.oms.api.auth.ApiKeyAuthenticationProvider;
import com.openexchange.oms.api.auth.HttpAuthHandler;
import com.openexchange.oms.api.auth.RoleBasedAuthorizer;
import com.openexchange.oms.api.dto.CreateOrderResponse;
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

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * History read endpoints + duplicate-create semantics (oms#40): routing,
 * identity/paging params, and the 503 UNAVAILABLE degradation when the OMS
 * runs without persistence.
 */
class RestApiHandlerHistoryTest {

    // userA-key → user 1 (plain), admin-key → user 2 with ADMIN
    private static final ApiKeyAuthenticationProvider PROVIDER =
            ApiKeyAuthenticationProvider.parse("userA-key:1;admin-key:2:ADMIN", "");

    private OrderService orderService;

    @BeforeEach
    void setUp() {
        orderService = mock(OrderService.class);
    }

    private EmbeddedChannel channel() {
        return new EmbeddedChannel(
                new HttpAuthHandler(PROVIDER),
                new RestApiHandler(orderService, mock(AdminService.class), new RoleBasedAuthorizer(),
                        CorsPolicy.fromSpec(""), com.openexchange.oms.api.audit.AuditLog.disabled(),
                        new io.micrometer.prometheusmetrics.PrometheusMeterRegistry(
                                io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT)));
    }

    private static FullHttpRequest request(HttpMethod method, String uri, String body, String apiKey) {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri,
                body == null ? Unpooled.EMPTY_BUFFER
                        : Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
        if (apiKey != null) req.headers().set("X-API-Key", apiKey);
        return req;
    }

    private FullHttpResponse exchange(FullHttpRequest req) {
        EmbeddedChannel ch = channel();
        ch.writeInbound(req);
        FullHttpResponse resp = ch.readOutbound();
        assertNotNull(resp, "expected an HTTP response");
        return resp;
    }

    private static String body(FullHttpResponse resp) {
        return resp.content().toString(StandardCharsets.UTF_8);
    }

    @Test
    void orderHistoryRoutesWithDefaultsAndPrincipalIdentity() {
        when(orderService.getOrderHistory(anyLong(), any(), anyInt(), anyInt())).thenReturn(List.of());
        FullHttpResponse resp = exchange(request(HttpMethod.GET, "/api/v1/orders/history", null, "userA-key"));
        assertEquals(HttpResponseStatus.OK, resp.status());
        // default paging, identity from the principal, no status filter
        verify(orderService).getOrderHistory(1L, null, 100, 0);
    }

    @Test
    void orderHistoryPassesStatusAndPaging() {
        when(orderService.getOrderHistory(anyLong(), any(), anyInt(), anyInt())).thenReturn(List.of());
        exchange(request(HttpMethod.GET,
                "/api/v1/orders/history?status=FILLED&limit=20&offset=40", null, "userA-key"));
        verify(orderService).getOrderHistory(1L, "FILLED", 20, 40);
    }

    @Test
    void historyEndpointsEnforceOwnership() {
        assertEquals(HttpResponseStatus.FORBIDDEN,
                exchange(request(HttpMethod.GET, "/api/v1/orders/history?userId=2", null, "userA-key")).status());
        assertEquals(HttpResponseStatus.FORBIDDEN,
                exchange(request(HttpMethod.GET, "/api/v1/executions?userId=2", null, "userA-key")).status());
        assertEquals(HttpResponseStatus.FORBIDDEN,
                exchange(request(HttpMethod.GET, "/api/v1/positions?userId=2", null, "userA-key")).status());
        verify(orderService, never()).getOrderHistory(anyLong(), any(), anyInt(), anyInt());
        verify(orderService, never()).getExecutions(anyLong(), anyInt(), anyInt());
        verify(orderService, never()).getPositions(anyLong());
    }

    @Test
    void malformedPagingIs400() {
        FullHttpResponse resp = exchange(request(HttpMethod.GET,
                "/api/v1/orders/history?limit=abc", null, "userA-key"));
        assertEquals(HttpResponseStatus.BAD_REQUEST, resp.status());
        assertTrue(body(resp).contains("VALIDATION"));

        assertEquals(HttpResponseStatus.BAD_REQUEST, exchange(request(HttpMethod.GET,
                "/api/v1/executions?offset=-1", null, "userA-key")).status());
        verify(orderService, never()).getOrderHistory(anyLong(), any(), anyInt(), anyInt());
        verify(orderService, never()).getExecutions(anyLong(), anyInt(), anyInt());
    }

    @Test
    void unknownStatusIs400() {
        when(orderService.getOrderHistory(anyLong(), any(), anyInt(), anyInt()))
                .thenThrow(new IllegalArgumentException("no enum constant"));
        FullHttpResponse resp = exchange(request(HttpMethod.GET,
                "/api/v1/orders/history?status=NOPE", null, "userA-key"));
        assertEquals(HttpResponseStatus.BAD_REQUEST, resp.status());
        assertTrue(body(resp).contains("VALIDATION"));
    }

    @Test
    void withoutPersistenceHistoryIs503Unavailable() {
        IllegalStateException noPg = new IllegalStateException("History unavailable");
        when(orderService.getOrderHistory(anyLong(), any(), anyInt(), anyInt())).thenThrow(noPg);
        when(orderService.getExecutions(anyLong(), anyInt(), anyInt())).thenThrow(noPg);
        when(orderService.getPositions(anyLong())).thenThrow(noPg);

        for (String uri : new String[]{"/api/v1/orders/history", "/api/v1/executions", "/api/v1/positions"}) {
            FullHttpResponse resp = exchange(request(HttpMethod.GET, uri, null, "userA-key"));
            assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE, resp.status(), uri);
            assertTrue(body(resp).contains("UNAVAILABLE"), uri);
        }
    }

    @Test
    void executionsAndPositionsRoute() {
        when(orderService.getExecutions(anyLong(), anyInt(), anyInt())).thenReturn(List.of());
        when(orderService.getPositions(anyLong())).thenReturn(List.of());

        assertEquals(HttpResponseStatus.OK,
                exchange(request(HttpMethod.GET, "/api/v1/executions?limit=5", null, "userA-key")).status());
        verify(orderService).getExecutions(1L, 5, 0);

        assertEquals(HttpResponseStatus.OK,
                exchange(request(HttpMethod.GET, "/api/v1/positions", null, "userA-key")).status());
        verify(orderService).getPositions(1L);
    }

    @Test
    void duplicateCreateReturns200NotCreated() {
        when(orderService.createOrder(any())).thenReturn(CreateOrderResponse.duplicate(42L, "NEW"));
        FullHttpResponse resp = exchange(request(HttpMethod.POST, "/api/v1/orders",
                "{\"marketId\":1,\"side\":\"BUY\",\"orderType\":\"LIMIT\",\"price\":\"100\",\"quantity\":\"1\","
                        + "\"clientOrderId\":\"abc\"}", "userA-key"));
        assertEquals(HttpResponseStatus.OK, resp.status());
        assertTrue(body(resp).contains("\"duplicate\":true"));
    }
}
