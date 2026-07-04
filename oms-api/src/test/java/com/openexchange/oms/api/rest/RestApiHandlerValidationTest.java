package com.openexchange.oms.api.rest;

import com.openexchange.oms.api.AdminService;
import com.openexchange.oms.api.OrderService;
import com.openexchange.oms.api.audit.AuditLog;
import com.openexchange.oms.api.auth.DevAuthenticationProvider;
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

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/** Edge input validation (oms#37): malformed input dies with 400, not 500. */
class RestApiHandlerValidationTest {

    private OrderService orderService;

    @BeforeEach
    void setUp() {
        orderService = mock(OrderService.class);
    }

    private FullHttpResponse exchange(HttpMethod method, String uri, String body) {
        EmbeddedChannel ch = new EmbeddedChannel(
                new HttpAuthHandler(new DevAuthenticationProvider()),
                new RestApiHandler(orderService, mock(AdminService.class),
                        new RoleBasedAuthorizer(), CorsPolicy.fromSpec(""), AuditLog.disabled(),
                        new io.micrometer.prometheusmetrics.PrometheusMeterRegistry(
                                io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT)));
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri,
                body == null ? Unpooled.EMPTY_BUFFER : Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
        ch.writeInbound(req);
        FullHttpResponse resp = ch.readOutbound();
        assertNotNull(resp, "expected a response");
        return resp;
    }

    @Test
    void depositMissingFieldsIs400Not500() {
        assertEquals(HttpResponseStatus.BAD_REQUEST,
                exchange(HttpMethod.POST, "/api/v1/accounts/1/deposit", "{}").status());
        assertEquals(HttpResponseStatus.BAD_REQUEST,
                exchange(HttpMethod.POST, "/api/v1/accounts/1/deposit", "{\"assetId\":1}").status());
        verify(orderService, never()).deposit(anyLong(), anyInt(), anyLong());
    }

    @Test
    void depositRejectsNonFiniteAndNonPositiveAmounts() {
        for (String amount : new String[]{"1e999", "-5", "0", "9.5e10"}) {
            assertEquals(HttpResponseStatus.BAD_REQUEST,
                    exchange(HttpMethod.POST, "/api/v1/accounts/1/deposit",
                            "{\"assetId\":1,\"amount\":" + amount + "}").status(),
                    "amount=" + amount);
        }
        verify(orderService, never()).deposit(anyLong(), anyInt(), anyLong());
    }

    @Test
    void depositAcceptsAssetIdZeroUsd() {
        // Asset ids are 0-based (USD=0); the oms#37 lower bound of 1 broke
        // USD deposits until oms#40. Negative stays rejected.
        assertEquals(HttpResponseStatus.OK,
                exchange(HttpMethod.POST, "/api/v1/accounts/1/deposit",
                        "{\"assetId\":0,\"amount\":\"100\"}").status());
        verify(orderService).deposit(1L, 0, 10_000_000_000L);
        assertEquals(HttpResponseStatus.BAD_REQUEST,
                exchange(HttpMethod.POST, "/api/v1/accounts/1/deposit",
                        "{\"assetId\":-1,\"amount\":\"100\"}").status());
    }

    @Test
    void withdrawRejectsFractionalAssetId() {
        assertEquals(HttpResponseStatus.BAD_REQUEST,
                exchange(HttpMethod.POST, "/api/v1/accounts/1/withdraw",
                        "{\"assetId\":1.5,\"amount\":10}").status());
        verify(orderService, never()).withdraw(anyLong(), anyInt(), anyLong());
    }

    @Test
    void createOrderRejectsInfinitePriceAndOverlongClientOrderId() {
        assertEquals(HttpResponseStatus.BAD_REQUEST,
                exchange(HttpMethod.POST, "/api/v1/orders",
                        "{\"marketId\":1,\"side\":\"BUY\",\"orderType\":\"LIMIT\",\"price\":1e999,\"quantity\":1}").status());
        assertEquals(HttpResponseStatus.BAD_REQUEST,
                exchange(HttpMethod.POST, "/api/v1/orders",
                        "{\"marketId\":1,\"side\":\"BUY\",\"orderType\":\"LIMIT\",\"price\":1,\"quantity\":1,"
                                + "\"clientOrderId\":\"" + "x".repeat(65) + "\"}").status());
        verify(orderService, never()).createOrder(any());
    }

    @Test
    void updateOrderRejectsInfiniteValues() {
        assertEquals(HttpResponseStatus.BAD_REQUEST,
                exchange(HttpMethod.PUT, "/api/v1/orders/5", "{\"price\":1e999}").status());
        verify(orderService, never()).updateOrder(anyLong(), anyLong(), anyLong());
    }
}
