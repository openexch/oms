// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.rest;

import com.openexchange.oms.api.AdminService;
import com.openexchange.oms.api.OrderService;
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
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * /api/v1/auth/* endpoints (OMS_AUTH_MODE=demo): register/login/me routing,
 * auth-gate exemption, error mapping (400/401/409/503), and that responses
 * (produced on the auth executor) reach the wire.
 */
class RestApiHandlerDemoAuthTest {

    /** Deterministic in-memory AuthService standing in for DemoAuthService. */
    private static final AuthService FAKE = new AuthService() {
        @Override
        public Session register(String username, String password) {
            if (username == null || username.length() < 3) {
                throw new IllegalArgumentException("Username must be 3-20 characters");
            }
            if ("taken".equals(username)) {
                throw new UsernameTakenException(username);
            }
            return new Session(100001, username, "tok-" + username);
        }

        @Override
        public Session login(String username, String password) {
            return "alice".equals(username) && "hunter22".equals(password)
                    ? new Session(100001, "alice", "tok-fresh")
                    : null;
        }

        @Override
        public Principal authenticateToken(String token) throws AuthenticationException {
            if ("tok-alice".equals(token)) {
                return new Principal("user-alice", 100001, Set.of());
            }
            throw new AuthenticationException("Invalid or expired token");
        }
    };

    private EmbeddedChannel channel(AuthService authService) {
        RestApiHandler handler = new RestApiHandler(mock(OrderService.class), mock(AdminService.class),
                new RoleBasedAuthorizer(), CorsPolicy.fromSpec(""),
                com.openexchange.oms.api.audit.AuditLog.disabled(),
                new io.micrometer.prometheusmetrics.PrometheusMeterRegistry(
                        io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT));
        handler.setAuthService(authService);
        return new EmbeddedChannel(new HttpAuthHandler(new DemoAuthenticationProvider(authService)), handler);
    }

    private static FullHttpRequest request(HttpMethod method, String uri, String body, String bearer) {
        FullHttpRequest req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri,
                body == null ? Unpooled.EMPTY_BUFFER
                        : Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
        if (bearer != null) {
            req.headers().set("Authorization", "Bearer " + bearer);
        }
        req.headers().set("Content-Length", req.content().readableBytes());
        return req;
    }

    /** Auth responses are written from the executor thread — poll for them. */
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
    void register_success_returnsSession_withoutAnyToken() throws Exception {
        EmbeddedChannel ch = channel(FAKE);
        ch.writeInbound(request(HttpMethod.POST, "/api/v1/auth/register",
                "{\"username\":\"alice\",\"password\":\"hunter22\"}", null)); // no Authorization: exempt route
        FullHttpResponse response = awaitResponse(ch);
        assertEquals(HttpResponseStatus.OK, response.status());
        String body = bodyOf(response);
        assertTrue(body.contains("\"userId\":100001"));
        assertTrue(body.contains("\"username\":\"alice\""));
        assertTrue(body.contains("\"token\":\"tok-alice\"") || body.contains("\"token\":\"tok-"));
    }

    @Test
    void register_invalidUsername_400() throws Exception {
        EmbeddedChannel ch = channel(FAKE);
        ch.writeInbound(request(HttpMethod.POST, "/api/v1/auth/register",
                "{\"username\":\"x\",\"password\":\"hunter22\"}", null));
        FullHttpResponse response = awaitResponse(ch);
        assertEquals(HttpResponseStatus.BAD_REQUEST, response.status());
        assertTrue(bodyOf(response).contains("VALIDATION"));
    }

    @Test
    void register_usernameTaken_409() throws Exception {
        EmbeddedChannel ch = channel(FAKE);
        ch.writeInbound(request(HttpMethod.POST, "/api/v1/auth/register",
                "{\"username\":\"taken\",\"password\":\"hunter22\"}", null));
        FullHttpResponse response = awaitResponse(ch);
        assertEquals(HttpResponseStatus.CONFLICT, response.status());
    }

    @Test
    void login_wrongPassword_401() throws Exception {
        EmbeddedChannel ch = channel(FAKE);
        ch.writeInbound(request(HttpMethod.POST, "/api/v1/auth/login",
                "{\"username\":\"alice\",\"password\":\"wrong\"}", null));
        FullHttpResponse response = awaitResponse(ch);
        assertEquals(HttpResponseStatus.UNAUTHORIZED, response.status());
        assertTrue(bodyOf(response).contains("UNAUTHORIZED"));
    }

    @Test
    void login_success_rotatedToken() throws Exception {
        EmbeddedChannel ch = channel(FAKE);
        ch.writeInbound(request(HttpMethod.POST, "/api/v1/auth/login",
                "{\"username\":\"alice\",\"password\":\"hunter22\"}", null));
        FullHttpResponse response = awaitResponse(ch);
        assertEquals(HttpResponseStatus.OK, response.status());
        assertTrue(bodyOf(response).contains("\"token\":\"tok-fresh\""));
    }

    @Test
    void authRoutes_withoutAuthService_503() throws Exception {
        EmbeddedChannel ch = channel(null);
        ch.writeInbound(request(HttpMethod.POST, "/api/v1/auth/register",
                "{\"username\":\"alice\",\"password\":\"hunter22\"}", null));
        // Synchronous path (no executor dispatch when authService is null)
        ch.runPendingTasks();
        FullHttpResponse response = ch.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE, response.status());
        assertTrue(bodyOf(response).contains("UNAVAILABLE"));
    }

    @Test
    void me_withRegisteredToken_echoesIdentity() throws Exception {
        EmbeddedChannel ch = channel(FAKE);
        ch.writeInbound(request(HttpMethod.GET, "/api/v1/auth/me", null, "tok-alice"));
        ch.runPendingTasks();
        FullHttpResponse response = ch.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.OK, response.status());
        String body = bodyOf(response);
        assertTrue(body.contains("\"userId\":100001"));
        assertTrue(body.contains("\"username\":\"alice\""));
    }

    @Test
    void me_withoutToken_401FromAuthGate() {
        EmbeddedChannel ch = channel(FAKE);
        ch.writeInbound(request(HttpMethod.GET, "/api/v1/auth/me", null, null));
        ch.runPendingTasks();
        FullHttpResponse response = ch.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.UNAUTHORIZED, response.status());
    }

    @Test
    void scopedEndpoints_rejectDevTokenForRegisteredRange() {
        EmbeddedChannel ch = channel(FAKE);
        ch.writeInbound(request(HttpMethod.GET, "/api/v1/orders", null, "dev:100001"));
        ch.runPendingTasks();
        FullHttpResponse response = ch.readOutbound();
        assertNotNull(response);
        assertEquals(HttpResponseStatus.UNAUTHORIZED, response.status());
    }
}
