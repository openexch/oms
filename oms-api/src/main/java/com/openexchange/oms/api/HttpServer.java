package com.openexchange.oms.api;

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
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public HttpServer(int port, OrderService orderService, WebSocketHandler webSocketHandler) {
        this(port, orderService, webSocketHandler, null);
    }

    public HttpServer(int port, OrderService orderService, WebSocketHandler webSocketHandler, AdminService adminService) {
        this.port = port;
        this.orderService = orderService;
        this.webSocketHandler = webSocketHandler;
        this.adminService = adminService;
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
                    pipeline.addLast(new HttpServerCodec());
                    pipeline.addLast(new HttpObjectAggregator(1048576)); // 1MB max
                    // Route: /ws/v1 → WebSocket, everything else → REST
                    pipeline.addLast(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                            if (msg instanceof FullHttpRequest req) {
                                if (req.uri().startsWith("/ws/v1")) {
                                    // Upgrade to WebSocket
                                    ctx.pipeline().addLast(new WebSocketServerProtocolHandler("/ws/v1"));
                                    ctx.pipeline().addLast(webSocketHandler);
                                    ctx.pipeline().remove(this);
                                    ctx.fireChannelRead(msg);
                                } else {
                                    // REST handler
                                    ctx.pipeline().addLast(new RestApiHandler(orderService, adminService));
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
}
