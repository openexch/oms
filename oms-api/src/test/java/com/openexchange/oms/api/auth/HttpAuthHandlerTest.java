// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.auth;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HttpAuthHandlerTest {

    private static final ApiKeyAuthenticationProvider PROVIDER =
            ApiKeyAuthenticationProvider.parse("good-key:7", "");

    private static FullHttpRequest get(String uri) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri, Unpooled.EMPTY_BUFFER);
    }

    @Test
    void unauthenticatedRequestGets401() {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpAuthHandler(PROVIDER));
        ch.writeInbound(get("/api/v1/orders?userId=7"));
        FullHttpResponse resp = ch.readOutbound();
        assertEquals(HttpResponseStatus.UNAUTHORIZED, resp.status());
        assertNull(ch.attr(HttpAuthHandler.PRINCIPAL).get());
        resp.release();
    }

    @Test
    void validKeyPassesThroughWithPrincipal() {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpAuthHandler(PROVIDER));
        FullHttpRequest req = get("/api/v1/orders?userId=7");
        req.headers().set("X-API-Key", "good-key");
        ch.writeInbound(req);
        assertNull(ch.readOutbound(), "no auth response — request forwarded");
        FullHttpRequest forwarded = ch.readInbound();
        assertNotNull(forwarded);
        assertEquals(7, ch.attr(HttpAuthHandler.PRINCIPAL).get().userId());
        forwarded.release();
    }

    @Test
    void healthIsExempt() {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpAuthHandler(PROVIDER));
        ch.writeInbound(get("/api/v1/health"));
        FullHttpRequest forwarded = ch.readInbound();
        assertNotNull(forwarded, "health must not require credentials");
        forwarded.release();
    }

    @Test
    void corsPreflightIsExempt() {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpAuthHandler(PROVIDER));
        ch.writeInbound(new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.OPTIONS, "/api/v1/orders", Unpooled.EMPTY_BUFFER));
        FullHttpRequest forwarded = ch.readInbound();
        assertNotNull(forwarded);
        forwarded.release();
    }

    @Test
    void wsUpgradeAcceptsBearerSubprotocolToken() {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpAuthHandler(PROVIDER));
        FullHttpRequest req = get("/ws/v1");
        req.headers().set("Sec-WebSocket-Protocol", "bearer, good-key");
        ch.writeInbound(req);
        FullHttpRequest forwarded = ch.readInbound();
        assertNotNull(forwarded, "ws subprotocol token must authenticate the upgrade");
        assertEquals(7, ch.attr(HttpAuthHandler.PRINCIPAL).get().userId());
        forwarded.release();
    }

    @Test
    void wsUpgradeRejectsQueryTokenAndMissingCredentials() {
        // Tokens in the URL leak into logs/history — must NOT authenticate.
        EmbeddedChannel ch = new EmbeddedChannel(new HttpAuthHandler(PROVIDER));
        ch.writeInbound(get("/ws/v1?token=good-key"));
        FullHttpResponse resp = ch.readOutbound();
        assertEquals(HttpResponseStatus.UNAUTHORIZED, resp.status());
        resp.release();

        ch = new EmbeddedChannel(new HttpAuthHandler(PROVIDER));
        ch.writeInbound(get("/ws/v1"));
        resp = ch.readOutbound();
        assertEquals(HttpResponseStatus.UNAUTHORIZED, resp.status());
        resp.release();
    }

    @Test
    void wsSubprotocolWithoutBearerMarkerIsNotAToken() {
        EmbeddedChannel ch = new EmbeddedChannel(new HttpAuthHandler(PROVIDER));
        FullHttpRequest req = get("/ws/v1");
        req.headers().set("Sec-WebSocket-Protocol", "good-key");
        ch.writeInbound(req);
        FullHttpResponse resp = ch.readOutbound();
        assertEquals(HttpResponseStatus.UNAUTHORIZED, resp.status());
        resp.release();
    }
}
