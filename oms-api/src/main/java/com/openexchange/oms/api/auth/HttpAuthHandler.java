// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.auth;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.AttributeKey;

import com.openexchange.oms.api.rest.CorsPolicy;

import java.nio.charset.StandardCharsets;

/**
 * Authenticates every HTTP request (REST and the WebSocket upgrade) before
 * routing, and attaches the {@link Principal} to the channel. Failures get a
 * 401 and the connection closes. Exempt: CORS preflight and GET /api/v1/health
 * (process-manager liveness probes must not need credentials).
 *
 * WebSocket note: browsers cannot set arbitrary headers on the upgrade
 * request, but they CAN set Sec-WebSocket-Protocol. A client that offers the
 * subprotocol list ["bearer", "&lt;token&gt;"] is treated as sending
 * Authorization: Bearer &lt;token&gt; (the server selects "bearer"; the token
 * entry is never echoed). Tokens are NOT accepted in the query string —
 * URLs leak into access logs, proxies, and browser history.
 */
public final class HttpAuthHandler extends ChannelInboundHandlerAdapter {

    public static final AttributeKey<Principal> PRINCIPAL = AttributeKey.valueOf("authPrincipal");

    private final AuthenticationProvider provider;
    private final CorsPolicy corsPolicy;

    public HttpAuthHandler(AuthenticationProvider provider) {
        this(provider, CorsPolicy.fromSpec(""));
    }

    public HttpAuthHandler(AuthenticationProvider provider, CorsPolicy corsPolicy) {
        this.provider = provider;
        this.corsPolicy = corsPolicy;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof FullHttpRequest req)) {
            ctx.fireChannelRead(msg);
            return;
        }
        if (req.method() == HttpMethod.OPTIONS || isExempt(req.uri())) {
            ctx.fireChannelRead(msg);
            return;
        }
        try {
            Principal principal = provider.authenticate(headersOf(req));
            ctx.channel().attr(PRINCIPAL).set(principal);
            ctx.fireChannelRead(msg);
        } catch (AuthenticationException e) {
            String origin = req.headers().get(HttpHeaderNames.ORIGIN);
            req.release();
            sendUnauthorized(ctx, origin, e.getMessage());
        }
    }

    private static boolean isExempt(String uri) {
        String path = new QueryStringDecoder(uri).path();
        // /metrics is for the local Prometheus scraper — like health, it must
        // work without credentials. Never proxy it publicly (docs/deploy-tls.md).
        return path.equals("/api/v1/health") || path.equals("/metrics");
    }

    private static AuthenticationProvider.Headers headersOf(FullHttpRequest req) {
        String wsToken = wsSubprotocolToken(req);
        return name -> {
            String value = req.headers().get(name);
            if (value == null && wsToken != null && name.equalsIgnoreCase("Authorization")) {
                return "Bearer " + wsToken;
            }
            return value;
        };
    }

    /**
     * Browser WS credential passing: Sec-WebSocket-Protocol offer of
     * ["bearer", "&lt;token&gt;"] carries the token in a header (never the URL).
     */
    private static String wsSubprotocolToken(FullHttpRequest req) {
        if (!req.uri().startsWith("/ws/v1")) return null;
        String offered = req.headers().get(HttpHeaderNames.SEC_WEBSOCKET_PROTOCOL);
        if (offered == null) return null;
        String token = null;
        boolean bearer = false;
        for (String entry : offered.split(",")) {
            String value = entry.trim();
            if (value.equalsIgnoreCase("bearer")) {
                bearer = true;
            } else if (!value.isEmpty() && token == null) {
                token = value;
            }
        }
        return bearer ? token : null;
    }

    private void sendUnauthorized(ChannelHandlerContext ctx, String origin, String message) {
        String safe = message == null ? "Unauthorized" : message.replace("\\", "\\\\").replace("\"", "\\\"");
        byte[] body = ("{\"error\":\"" + safe + "\",\"code\":\"UNAUTHORIZED\"}").getBytes(StandardCharsets.UTF_8);
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED, Unpooled.wrappedBuffer(body));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length);
        response.headers().set(HttpHeaderNames.WWW_AUTHENTICATE, "Bearer");
        corsPolicy.apply(origin, response);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }
}
