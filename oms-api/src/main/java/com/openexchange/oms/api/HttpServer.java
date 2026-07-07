// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api;

import com.openexchange.oms.api.audit.AuditLog;
import com.openexchange.oms.api.auth.AuthenticationProvider;
import com.openexchange.oms.api.auth.Authorizer;
import com.openexchange.oms.api.auth.HttpAuthHandler;
import com.openexchange.oms.api.rest.CorsPolicy;
import com.openexchange.oms.api.rest.RestApiHandler;
import com.openexchange.oms.api.websocket.WebSocketHandler;
import com.openexchange.oms.api.AdminService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty-based HTTP/WebSocket server for OMS API.
 */
public class HttpServer {

    private static final Logger log = LoggerFactory.getLogger(HttpServer.class);

    private final int port;
    private final OrderService orderService;
    private final WebSocketHandler webSocketHandler;
    private final AdminService adminService;
    private final AuthenticationProvider authProvider;
    private final Authorizer authorizer;
    private final CorsPolicy corsPolicy;
    private final AuditLog auditLog;
    private final io.micrometer.prometheusmetrics.PrometheusMeterRegistry meterRegistry;
    // Demo auth (OMS_AUTH_MODE=demo); null in other modes
    private volatile com.openexchange.oms.api.auth.AuthService authService;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public HttpServer(int port, OrderService orderService, WebSocketHandler webSocketHandler,
                      AdminService adminService, AuthenticationProvider authProvider, Authorizer authorizer,
                      CorsPolicy corsPolicy, AuditLog auditLog,
                      io.micrometer.prometheusmetrics.PrometheusMeterRegistry meterRegistry) {
        this.port = port;
        this.orderService = orderService;
        this.webSocketHandler = webSocketHandler;
        this.adminService = adminService;
        this.authProvider = authProvider;
        this.authorizer = authorizer;
        this.corsPolicy = corsPolicy;
        this.auditLog = auditLog;
        this.meterRegistry = meterRegistry;
    }

    /** Demo auth service for the /api/v1/auth/* endpoints. Set before start(). */
    public void setAuthService(com.openexchange.oms.api.auth.AuthService authService) {
        this.authService = authService;
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ChannelPipeline pipeline = ch.pipeline();
                    // Reap pooled-but-abandoned keep-alive connections (oms#66):
                    // 60s all-idle so a client that pooled a socket and walked
                    // away does not leak an FD. Placed before the codec so it
                    // observes raw socket activity; the matching IdleCloseHandler
                    // does the close. Both are removed on a WebSocket upgrade —
                    // the server sends no WS heartbeats, so a quiet-but-healthy
                    // WS must not be reaped here.
                    pipeline.addLast(new IdleStateHandler(0, 0, 60));
                    pipeline.addLast(new IdleCloseHandler());
                    pipeline.addLast(new HttpServerCodec());
                    pipeline.addLast(new HttpObjectAggregator(1048576)); // 1MB max
                    // Auth gate: every request (incl. the WS upgrade) is
                    // authenticated here; the Principal rides on the channel.
                    pipeline.addLast(new HttpAuthHandler(authProvider, corsPolicy));
                    // Route: /ws/v1 → WebSocket, everything else → REST
                    pipeline.addLast(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                            if (msg instanceof FullHttpRequest req) {
                                if (req.uri().startsWith("/ws/v1")) {
                                    // Upgrade to WebSocket. "bearer" subprotocol is offered by
                                    // browser clients passing a token via Sec-WebSocket-Protocol
                                    // (see HttpAuthHandler); clients offering no subprotocol
                                    // still negotiate fine.
                                    // The WS upgrade is always the FIRST request on its own
                                    // connection (browsers open a fresh socket for it), so this
                                    // path is untouched by REST keep-alive. Drop the REST idle
                                    // reaper: WS liveness is the client's JSON ping, not TCP idle.
                                    ctx.pipeline().remove(IdleStateHandler.class);
                                    ctx.pipeline().remove(IdleCloseHandler.class);
                                    ctx.pipeline().addLast(new WebSocketServerProtocolHandler("/ws/v1", "bearer"));
                                    ctx.pipeline().addLast(webSocketHandler);
                                    ctx.pipeline().remove(this);
                                    ctx.fireChannelRead(msg);
                                } else {
                                    // REST handler.
                                    // HAZARD 2 (oms#66): this router removes itself after the
                                    // first REST request, so on a reused keep-alive connection a
                                    // later /ws/v1 would reach RestApiHandler and 404 instead of
                                    // upgrading. Accepted and documented: browsers open a fresh
                                    // socket per WS upgrade, so a connection is REST-only or
                                    // WS-only in practice. If mixed reuse ever becomes real, keep
                                    // the router in place and dispatch per request instead.
                                    RestApiHandler restHandler = new RestApiHandler(orderService, adminService,
                                            authorizer, corsPolicy, auditLog, meterRegistry);
                                    restHandler.setAuthService(authService);
                                    ctx.pipeline().addLast(restHandler);
                                    ctx.pipeline().remove(this);
                                    ctx.fireChannelRead(msg);
                                }
                            } else {
                                ctx.fireChannelRead(msg);
                            }
                        }
                    });
                }
            })
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true);

        serverChannel = bootstrap.bind(port).sync().channel();
        log.info("OMS HTTP/WebSocket server started on port {}", port);
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        log.info("OMS HTTP server stopped");
    }

    public WebSocketHandler getWebSocketHandler() {
        return webSocketHandler;
    }

    /**
     * Closes a connection once {@link IdleStateHandler} reports it idle past the
     * window (oms#66): reaps keep-alive sockets a client pooled and abandoned so
     * they do not leak file descriptors. Removed on WebSocket upgrade.
     */
    private static final class IdleCloseHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                ctx.close();
            } else {
                ctx.fireUserEventTriggered(evt);
            }
        }
    }
}
