// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.rest;

import com.openexchange.oms.api.AdminService;
import com.openexchange.oms.api.OrderService;
import com.openexchange.oms.api.auth.ApiKeyAuthenticationProvider;
import com.openexchange.oms.api.auth.AuthService;
import com.openexchange.oms.api.auth.AuthenticationException;
import com.openexchange.oms.api.auth.DemoAuthenticationProvider;
import com.openexchange.oms.api.auth.HttpAuthHandler;
import com.openexchange.oms.api.auth.Principal;
import com.openexchange.oms.api.auth.RoleBasedAuthorizer;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Real HTTP/1.1 keep-alive (oms#66): every response honors the client's
 * Connection preference and the socket is closed only when the caller asked
 * to (or spoke HTTP/1.0 without keep-alive). Also pins the HAZARD 1 fix — the
 * CORS Origin is captured per request, so the deferred async-auth response
 * cannot pick up a later request's Origin.
 */
class RestApiHandlerKeepAliveTest {

    // userA-key → user 1 (plain)
    private static final ApiKeyAuthenticationProvider PROVIDER =
            ApiKeyAuthenticationProvider.parse("userA-key:1", "");

    private OrderService orderService() {
        OrderService orderService = mock(OrderService.class);
        when(orderService.isClusterConnected()).thenReturn(true);
        when(orderService.getActiveOrderCount()).thenReturn(0);
        return orderService;
    }

    private EmbeddedChannel channel(OrderService orderService) {
        return channel(orderService, CorsPolicy.fromSpec(""), null);
    }

    private EmbeddedChannel channel(OrderService orderService, CorsPolicy corsPolicy, AuthService authService) {
        RestApiHandler handler = new RestApiHandler(orderService, mock(AdminService.class),
                new RoleBasedAuthorizer(), corsPolicy,
                com.openexchange.oms.api.audit.AuditLog.disabled(),
                new io.micrometer.prometheusmetrics.PrometheusMeterRegistry(
                        io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT));
        handler.setAuthService(authService);
        return new EmbeddedChannel(
                authService != null ? new HttpAuthHandler(new DemoAuthenticationProvider(authService))
                        : new HttpAuthHandler(PROVIDER),
                handler);
    }

    private static FullHttpRequest request(HttpVersion version, HttpMethod method, String uri,
                                           String body, String apiKey, String connection, String origin) {
        FullHttpRequest req = new DefaultFullHttpRequest(version, method, uri,
                body == null ? Unpooled.EMPTY_BUFFER : Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
        if (apiKey != null) req.headers().set("X-API-Key", apiKey);
        if (connection != null) req.headers().set(HttpHeaderNames.CONNECTION, connection);
        if (origin != null) req.headers().set(HttpHeaderNames.ORIGIN, origin);
        req.headers().set(HttpHeaderNames.CONTENT_LENGTH, req.content().readableBytes());
        return req;
    }

    /** Poll the executor-driven writes until a response lands (async auth path). */
    private static FullHttpResponse awaitResponse(EmbeddedChannel channel) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            channel.runPendingTasks();
            FullHttpResponse response = channel.readOutbound();
            if (response != null) return response;
            Thread.sleep(5);
        }
        fail("no response within 5s");
        return null;
    }

    // ==================== keep-alive reuse ====================

    @Test
    void twoSequentialHttp11RequestsShareOneChannelWhichStaysOpen() {
        EmbeddedChannel ch = channel(orderService());

        ch.writeInbound(request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/api/v1/health",
                null, null, null, null));
        FullHttpResponse first = ch.readOutbound();
        assertNotNull(first);
        assertEquals(HttpResponseStatus.OK, first.status());
        assertTrue(HttpUtil.isKeepAlive(first), "HTTP/1.1 default is keep-alive");
        assertTrue(ch.isOpen(), "connection must stay open for reuse");
        first.release();

        // Reuse the SAME channel for a second request.
        ch.writeInbound(request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/api/v1/health",
                null, null, null, null));
        FullHttpResponse second = ch.readOutbound();
        assertNotNull(second, "second request on the reused connection must be answered");
        assertEquals(HttpResponseStatus.OK, second.status());
        assertTrue(HttpUtil.isKeepAlive(second));
        assertTrue(ch.isOpen());
        second.release();
    }

    @Test
    void connectionCloseRequestGetsCloseHeaderAndClosesChannel() {
        EmbeddedChannel ch = channel(orderService());
        ch.writeInbound(request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/api/v1/health",
                null, null, "close", null));
        FullHttpResponse resp = ch.readOutbound();
        assertNotNull(resp);
        assertEquals(HttpResponseStatus.OK, resp.status());
        assertFalse(HttpUtil.isKeepAlive(resp));
        assertEquals("close", resp.headers().get(HttpHeaderNames.CONNECTION));
        assertFalse(ch.isOpen(), "explicit Connection: close must close the socket");
        resp.release();
    }

    @Test
    void http10WithoutKeepAliveCloses() {
        EmbeddedChannel ch = channel(orderService());
        ch.writeInbound(request(HttpVersion.HTTP_1_0, HttpMethod.GET, "/api/v1/health",
                null, null, null, null));
        FullHttpResponse resp = ch.readOutbound();
        assertNotNull(resp);
        assertEquals(HttpResponseStatus.OK, resp.status());
        assertFalse(HttpUtil.isKeepAlive(resp), "HTTP/1.0 without keep-alive must not pool");
        assertFalse(ch.isOpen());
        resp.release();
    }

    @Test
    void http10WithKeepAliveHeaderStaysOpen() {
        EmbeddedChannel ch = channel(orderService());
        ch.writeInbound(request(HttpVersion.HTTP_1_0, HttpMethod.GET, "/api/v1/health",
                null, null, "keep-alive", null));
        FullHttpResponse resp = ch.readOutbound();
        assertNotNull(resp);
        assertEquals(HttpResponseStatus.OK, resp.status());
        assertTrue(HttpUtil.isKeepAlive(resp));
        assertTrue(ch.isOpen());
        resp.release();
    }

    // ==================== preflight + error paths ====================

    @Test
    void corsPreflightHonorsKeepAlive() {
        EmbeddedChannel ka = channel(orderService());
        ka.writeInbound(request(HttpVersion.HTTP_1_1, HttpMethod.OPTIONS, "/api/v1/orders",
                null, null, null, null));
        FullHttpResponse kaResp = ka.readOutbound();
        assertNotNull(kaResp);
        assertEquals(HttpResponseStatus.NO_CONTENT, kaResp.status());
        assertTrue(HttpUtil.isKeepAlive(kaResp));
        assertTrue(ka.isOpen());
        kaResp.release();

        EmbeddedChannel close = channel(orderService());
        close.writeInbound(request(HttpVersion.HTTP_1_1, HttpMethod.OPTIONS, "/api/v1/orders",
                null, null, "close", null));
        FullHttpResponse closeResp = close.readOutbound();
        assertNotNull(closeResp);
        assertEquals(HttpResponseStatus.NO_CONTENT, closeResp.status());
        assertFalse(HttpUtil.isKeepAlive(closeResp));
        assertFalse(close.isOpen());
        closeResp.release();
    }

    @Test
    void errorResponseHonorsKeepAlive() {
        // Unknown path under /api/v1 → RestApiHandler 404 via sendError.
        EmbeddedChannel ka = channel(orderService());
        ka.writeInbound(request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/api/v1/nope",
                null, "userA-key", null, null));
        FullHttpResponse kaResp = ka.readOutbound();
        assertNotNull(kaResp);
        assertEquals(HttpResponseStatus.NOT_FOUND, kaResp.status());
        assertTrue(HttpUtil.isKeepAlive(kaResp), "an error is not a reason to drop the connection");
        assertTrue(ka.isOpen());
        kaResp.release();

        EmbeddedChannel close = channel(orderService());
        close.writeInbound(request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/api/v1/nope",
                null, "userA-key", "close", null));
        FullHttpResponse closeResp = close.readOutbound();
        assertNotNull(closeResp);
        assertEquals(HttpResponseStatus.NOT_FOUND, closeResp.status());
        assertFalse(HttpUtil.isKeepAlive(closeResp));
        assertFalse(close.isOpen());
        closeResp.release();
    }

    @Test
    void authGate401HonorsKeepAlive() {
        // 401 originates in HttpAuthHandler (before the router); it must also
        // honor keep-alive so an HTTP/1.1 client can retry on the same socket.
        EmbeddedChannel ch = channel(orderService());
        ch.writeInbound(request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/api/v1/orders?userId=1",
                null, null, null, null)); // no key → 401
        FullHttpResponse resp = ch.readOutbound();
        assertNotNull(resp);
        assertEquals(HttpResponseStatus.UNAUTHORIZED, resp.status());
        assertTrue(HttpUtil.isKeepAlive(resp));
        assertTrue(ch.isOpen());
        resp.release();
    }

    // ==================== HAZARD 1: async-auth Origin isolation ====================

    /**
     * The Origin is captured per request (oms#66 HAZARD 1): a response deferred
     * to the AUTH_EXECUTOR keeps the Origin from ITS request even when a later
     * request on the same keep-alive connection is served in the meantime.
     *
     * <p>Driven deterministically with a latch instead of racing two async
     * responses (EmbeddedChannel is single-thread affine): the register
     * response is held inside the auth executor until AFTER a second request
     * with a different Origin has been fully served. A shared Origin field
     * would have been clobbered by then; a per-request capture is not.
     */
    @Test
    void deferredAuthResponseKeepsItsOwnOrigin() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AuthService latching = new AuthService() {
            @Override
            public Session register(String username, String password) {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return new Session(100001, username, "tok-" + username);
            }

            @Override
            public Session login(String username, String password) {
                return null;
            }

            @Override
            public Principal authenticateToken(String token) throws AuthenticationException {
                throw new AuthenticationException("unused");
            }
        };

        CorsPolicy cors = CorsPolicy.fromSpec("http://register.test,http://later.test");
        EmbeddedChannel ch = channel(orderService(), cors, latching);

        // 1) Register (async, exempt route): its RequestContext captures Origin
        //    register.test. Block it inside the executor until step 3.
        ch.writeInbound(request(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api/v1/auth/register",
                "{\"username\":\"alice\",\"password\":\"secretpw\"}", null, null, "http://register.test"));
        assertTrue(entered.await(2, TimeUnit.SECONDS), "register must reach the auth executor");

        // 2) A second request on the SAME connection with a DIFFERENT Origin,
        //    served to completion while the register response is still pending.
        //    A shared Origin field would be overwritten to later.test here.
        ch.writeInbound(request(HttpVersion.HTTP_1_1, HttpMethod.GET, "/api/v1/health",
                null, null, null, "http://later.test"));
        FullHttpResponse health = ch.readOutbound();
        assertNotNull(health, "health must answer synchronously");
        assertEquals(HttpResponseStatus.OK, health.status());
        health.release();

        // 3) Release the deferred register response and check ITS Origin.
        release.countDown();
        FullHttpResponse registerResp = awaitResponse(ch);
        assertEquals(HttpResponseStatus.OK, registerResp.status());
        assertTrue(registerResp.content().toString(StandardCharsets.UTF_8).contains("\"alice\""),
                "the released response is the register response");
        assertEquals("http://register.test",
                registerResp.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN),
                "deferred response must echo its own request's Origin, not the later request's");
        registerResp.release();
    }
}
