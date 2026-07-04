package com.openexchange.oms.api.rest;

import com.openexchange.oms.api.AdminService;
import com.openexchange.oms.api.OrderService;
import com.openexchange.oms.api.auth.ApiKeyAuthenticationProvider;
import com.openexchange.oms.api.auth.HttpAuthHandler;
import com.openexchange.oms.api.auth.RoleBasedAuthorizer;
import com.openexchange.oms.api.dto.CreateOrderRequest;
import com.openexchange.oms.api.dto.CreateOrderResponse;
import com.openexchange.oms.api.dto.OrderResponse;
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
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Exit criterion for oms#36: unauthenticated → 401; user A cannot act on
 * user B via body/query/path userId or foreign order ids; admin routes need
 * the ADMIN role.
 */
class RestApiHandlerAuthTest {

    // userA-key → user 1 (plain), admin-key → user 2 with ADMIN
    private static final ApiKeyAuthenticationProvider PROVIDER =
            ApiKeyAuthenticationProvider.parse("userA-key:1;admin-key:2:ADMIN", "");

    private OrderService orderService;
    private AdminService adminService;

    @BeforeEach
    void setUp() {
        orderService = mock(OrderService.class);
        adminService = mock(AdminService.class);
    }

    private EmbeddedChannel channel() {
        return new EmbeddedChannel(
                new HttpAuthHandler(PROVIDER),
                new RestApiHandler(orderService, adminService, new RoleBasedAuthorizer(),
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

    @Test
    void unauthenticatedOrderPostIs401() {
        FullHttpResponse resp = exchange(request(HttpMethod.POST, "/api/v1/orders",
                "{\"userId\":1,\"marketId\":1}", null));
        assertEquals(HttpResponseStatus.UNAUTHORIZED, resp.status());
        verifyNoInteractions(orderService);
    }

    @Test
    void createOrderDerivesUserIdFromPrincipalWhenAbsent() {
        when(orderService.createOrder(any())).thenReturn(CreateOrderResponse.accepted(1, "NEW"));
        exchange(request(HttpMethod.POST, "/api/v1/orders",
                "{\"marketId\":1,\"side\":\"BUY\",\"orderType\":\"LIMIT\",\"price\":100,\"quantity\":1}", "userA-key"));

        ArgumentCaptor<CreateOrderRequest> captor = ArgumentCaptor.forClass(CreateOrderRequest.class);
        verify(orderService).createOrder(captor.capture());
        assertEquals(1, captor.getValue().getUserId());
    }

    @Test
    void createOrderAsOtherUserIs403() {
        FullHttpResponse resp = exchange(request(HttpMethod.POST, "/api/v1/orders",
                "{\"userId\":2,\"marketId\":1}", "userA-key"));
        assertEquals(HttpResponseStatus.FORBIDDEN, resp.status());
        verify(orderService, never()).createOrder(any());
    }

    @Test
    void queryOtherUsersOrdersIs403AndOwnDefaultsFromPrincipal() {
        FullHttpResponse resp = exchange(request(HttpMethod.GET, "/api/v1/orders?userId=2", null, "userA-key"));
        assertEquals(HttpResponseStatus.FORBIDDEN, resp.status());
        verify(orderService, never()).queryOrders(anyLong(), any());

        when(orderService.queryOrders(anyLong(), any())).thenReturn(List.of());
        exchange(request(HttpMethod.GET, "/api/v1/orders", null, "userA-key"));
        verify(orderService).queryOrders(eq(1L), any());
    }

    @Test
    void foreignOrderIsInvisibleAndUncancellable() {
        OrderResponse foreign = mock(OrderResponse.class);
        when(foreign.getUserId()).thenReturn(2L);
        when(orderService.getOrder(55L)).thenReturn(foreign);

        assertEquals(HttpResponseStatus.NOT_FOUND,
                exchange(request(HttpMethod.GET, "/api/v1/orders/55", null, "userA-key")).status());
        assertEquals(HttpResponseStatus.NOT_FOUND,
                exchange(request(HttpMethod.DELETE, "/api/v1/orders/55", null, "userA-key")).status());
        assertEquals(HttpResponseStatus.NOT_FOUND,
                exchange(request(HttpMethod.PUT, "/api/v1/orders/55", "{\"price\":2}", "userA-key")).status());
        verify(orderService, never()).cancelOrder(anyLong());
        verify(orderService, never()).updateOrder(anyLong(), anyDouble(), anyDouble());
    }

    @Test
    void accountRoutesEnforceOwnership() {
        assertEquals(HttpResponseStatus.FORBIDDEN,
                exchange(request(HttpMethod.GET, "/api/v1/accounts/2", null, "userA-key")).status());
        assertEquals(HttpResponseStatus.FORBIDDEN,
                exchange(request(HttpMethod.POST, "/api/v1/accounts/2/deposit",
                        "{\"assetId\":1,\"amount\":10}", "userA-key")).status());
        assertEquals(HttpResponseStatus.FORBIDDEN,
                exchange(request(HttpMethod.POST, "/api/v1/accounts/2/withdraw",
                        "{\"assetId\":1,\"amount\":10}", "userA-key")).status());
        verify(orderService, never()).deposit(anyLong(), anyInt(), anyLong());
        verify(orderService, never()).withdraw(anyLong(), anyInt(), anyLong());

        when(orderService.getBalances(1L)).thenReturn(Collections.emptyMap());
        assertEquals(HttpResponseStatus.OK,
                exchange(request(HttpMethod.GET, "/api/v1/accounts/1", null, "userA-key")).status());

        exchange(request(HttpMethod.POST, "/api/v1/accounts/1/deposit",
                "{\"assetId\":1,\"amount\":10}", "userA-key"));
        verify(orderService).deposit(eq(1L), eq(1), anyLong());
    }

    @Test
    void adminRoutesRequireAdminRole() {
        assertEquals(HttpResponseStatus.FORBIDDEN,
                exchange(request(HttpMethod.GET, "/api/v1/admin/risk/config", null, "userA-key")).status());
        verifyNoInteractions(adminService);

        when(adminService.getAllRiskConfigs()).thenReturn(Collections.emptyMap());
        assertEquals(HttpResponseStatus.OK,
                exchange(request(HttpMethod.GET, "/api/v1/admin/risk/config", null, "admin-key")).status());
        verify(adminService).getAllRiskConfigs();
    }
}
