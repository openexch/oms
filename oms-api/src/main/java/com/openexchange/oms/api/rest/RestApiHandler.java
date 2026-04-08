package com.openexchange.oms.api.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.openexchange.oms.api.AdminService;
import com.openexchange.oms.api.OrderService;
import com.openexchange.oms.api.dto.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Netty HTTP handler for REST API.
 * Routes: POST /api/v1/orders, DELETE /api/v1/orders/{id}, GET /api/v1/orders/{id},
 *         GET /api/v1/orders, GET /api/v1/accounts/{userId},
 *         POST /api/v1/accounts/{userId}/deposit, POST /api/v1/accounts/{userId}/withdraw,
 *         GET /api/v1/health, GET /api/v1/markets,
 *         GET/PUT /api/v1/admin/risk/config, POST /api/v1/admin/risk/circuit-breaker
 */
public class RestApiHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger log = LoggerFactory.getLogger(RestApiHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final OrderService orderService;
    private final AdminService adminService;

    public RestApiHandler(OrderService orderService) {
        this(orderService, null);
    }

    public RestApiHandler(OrderService orderService, AdminService adminService) {
        this.orderService = orderService;
        this.adminService = adminService;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        String uri = request.uri();
        HttpMethod method = request.method();

        try {
            if (uri.equals("/api/v1/health") && method == HttpMethod.GET) {
                handleHealth(ctx);
            } else if (uri.equals("/api/v1/orders") && method == HttpMethod.POST) {
                handleCreateOrder(ctx, request);
            } else if (uri.startsWith("/api/v1/orders/") && method == HttpMethod.DELETE) {
                handleCancelOrder(ctx, uri);
            } else if (uri.startsWith("/api/v1/orders/") && method == HttpMethod.GET) {
                handleGetOrder(ctx, uri);
            } else if (uri.startsWith("/api/v1/orders") && method == HttpMethod.GET) {
                handleQueryOrders(ctx, request);
            } else if (uri.matches("/api/v1/accounts/\\d+/deposit") && method == HttpMethod.POST) {
                handleDeposit(ctx, uri, request);
            } else if (uri.matches("/api/v1/accounts/\\d+/withdraw") && method == HttpMethod.POST) {
                handleWithdraw(ctx, uri, request);
            } else if (uri.startsWith("/api/v1/accounts/") && method == HttpMethod.GET) {
                handleGetAccount(ctx, uri);
            } else if (uri.equals("/api/v1/markets") && method == HttpMethod.GET) {
                handleGetMarkets(ctx);
            } else if (uri.startsWith("/api/v1/admin/risk/circuit-breaker/") && method == HttpMethod.POST) {
                handleCircuitBreaker(ctx, uri);
            } else if (uri.startsWith("/api/v1/admin/risk/config") && method == HttpMethod.GET) {
                handleGetRiskConfig(ctx, uri);
            } else if (uri.startsWith("/api/v1/admin/risk/config/") && method == HttpMethod.PUT) {
                handleUpdateRiskConfig(ctx, uri, request);
            } else {
                sendResponse(ctx, HttpResponseStatus.NOT_FOUND, "{\"error\":\"Not Found\"}");
            }
        } catch (Exception e) {
            log.error("Error handling request: {} {}", method, uri, e);
            sendResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
        }
    }

    private void handleHealth(ChannelHandlerContext ctx) throws Exception {
        ObjectNode health = MAPPER.createObjectNode();
        health.put("status", "UP");
        health.put("clusterConnected", orderService.isClusterConnected());
        health.put("activeOrders", orderService.getActiveOrderCount());
        sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(health));
    }

    private void handleCreateOrder(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        String body = request.content().toString(StandardCharsets.UTF_8);
        CreateOrderRequest req = MAPPER.readValue(body, CreateOrderRequest.class);

        CreateOrderResponse resp = orderService.createOrder(req);

        HttpResponseStatus status = resp.isAccepted()
            ? HttpResponseStatus.CREATED : HttpResponseStatus.BAD_REQUEST;
        sendResponse(ctx, status, MAPPER.writeValueAsString(resp));
    }

    private void handleCancelOrder(ChannelHandlerContext ctx, String uri) throws Exception {
        String idStr = uri.substring("/api/v1/orders/".length());
        long omsOrderId = Long.parseLong(idStr);
        CancelOrderResponse resp = orderService.cancelOrder(omsOrderId);
        sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(resp));
    }

    private void handleGetOrder(ChannelHandlerContext ctx, String uri) throws Exception {
        String idStr = uri.substring("/api/v1/orders/".length());
        long omsOrderId = Long.parseLong(idStr);
        OrderResponse resp = orderService.getOrder(omsOrderId);
        if (resp == null) {
            sendResponse(ctx, HttpResponseStatus.NOT_FOUND, "{\"error\":\"Order not found\"}");
        } else {
            sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(resp));
        }
    }

    private void handleQueryOrders(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        String userIdStr = decoder.parameters().containsKey("userId")
            ? decoder.parameters().get("userId").get(0) : null;
        String statusStr = decoder.parameters().containsKey("status")
            ? decoder.parameters().get("status").get(0) : null;

        if (userIdStr == null) {
            sendResponse(ctx, HttpResponseStatus.BAD_REQUEST, "{\"error\":\"userId required\"}");
            return;
        }

        long userId = Long.parseLong(userIdStr);
        var orders = orderService.queryOrders(userId, statusStr);
        sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(orders));
    }

    private void handleGetAccount(ChannelHandlerContext ctx, String uri) throws Exception {
        String idStr = uri.substring("/api/v1/accounts/".length());
        int slash = idStr.indexOf('/');
        if (slash > 0) {
            idStr = idStr.substring(0, slash);
        }
        long userId = Long.parseLong(idStr);
        var balances = orderService.getBalances(userId);
        sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(balances));
    }

    @SuppressWarnings("unchecked")
    private void handleDeposit(ChannelHandlerContext ctx, String uri, FullHttpRequest request) throws Exception {
        long userId = extractAccountUserId(uri);
        String body = request.content().toString(StandardCharsets.UTF_8);
        Map<String, Object> req = MAPPER.readValue(body, Map.class);

        int assetId = ((Number) req.get("assetId")).intValue();
        double amount = ((Number) req.get("amount")).doubleValue();
        long fpAmount = com.match.domain.FixedPoint.fromDouble(amount);

        orderService.deposit(userId, assetId, fpAmount);

        ObjectNode resp = MAPPER.createObjectNode();
        resp.put("success", true);
        resp.put("userId", userId);
        resp.put("assetId", assetId);
        resp.put("amount", amount);
        sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(resp));
    }

    @SuppressWarnings("unchecked")
    private void handleWithdraw(ChannelHandlerContext ctx, String uri, FullHttpRequest request) throws Exception {
        long userId = extractAccountUserId(uri);
        String body = request.content().toString(StandardCharsets.UTF_8);
        Map<String, Object> req = MAPPER.readValue(body, Map.class);

        int assetId = ((Number) req.get("assetId")).intValue();
        double amount = ((Number) req.get("amount")).doubleValue();
        long fpAmount = com.match.domain.FixedPoint.fromDouble(amount);

        try {
            orderService.withdraw(userId, assetId, fpAmount);
            ObjectNode resp = MAPPER.createObjectNode();
            resp.put("success", true);
            resp.put("userId", userId);
            resp.put("assetId", assetId);
            resp.put("amount", amount);
            sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(resp));
        } catch (IllegalStateException e) {
            sendResponse(ctx, HttpResponseStatus.BAD_REQUEST,
                "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
        }
    }

    private long extractAccountUserId(String uri) {
        // /api/v1/accounts/{userId}/deposit or /withdraw
        String path = uri.substring("/api/v1/accounts/".length());
        int slash = path.indexOf('/');
        return Long.parseLong(path.substring(0, slash));
    }

    private void handleGetMarkets(ChannelHandlerContext ctx) throws Exception {
        var markets = orderService.getMarkets();
        sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(markets));
    }

    // ==================== Admin Endpoints ====================

    private void handleGetRiskConfig(ChannelHandlerContext ctx, String uri) throws Exception {
        if (adminService == null) {
            sendResponse(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, "{\"error\":\"Admin not available\"}");
            return;
        }

        if (uri.equals("/api/v1/admin/risk/config")) {
            var configs = adminService.getAllRiskConfigs();
            sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(configs));
        } else {
            String idStr = uri.substring("/api/v1/admin/risk/config/".length());
            int marketId = Integer.parseInt(idStr);
            var config = adminService.getRiskConfig(marketId);
            if (config == null) {
                sendResponse(ctx, HttpResponseStatus.NOT_FOUND, "{\"error\":\"Market not configured\"}");
            } else {
                sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(config));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void handleUpdateRiskConfig(ChannelHandlerContext ctx, String uri, FullHttpRequest request) throws Exception {
        if (adminService == null) {
            sendResponse(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, "{\"error\":\"Admin not available\"}");
            return;
        }

        String idStr = uri.substring("/api/v1/admin/risk/config/".length());
        int marketId = Integer.parseInt(idStr);
        String body = request.content().toString(StandardCharsets.UTF_8);
        Map<String, Object> fields = MAPPER.readValue(body, Map.class);

        adminService.updateRiskConfig(marketId, fields);

        ObjectNode resp = MAPPER.createObjectNode();
        resp.put("success", true);
        resp.put("marketId", marketId);
        sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(resp));
    }

    private void handleCircuitBreaker(ChannelHandlerContext ctx, String uri) throws Exception {
        if (adminService == null) {
            sendResponse(ctx, HttpResponseStatus.SERVICE_UNAVAILABLE, "{\"error\":\"Admin not available\"}");
            return;
        }

        // /api/v1/admin/risk/circuit-breaker/{marketId}/trip or /reset
        String path = uri.substring("/api/v1/admin/risk/circuit-breaker/".length());
        String[] parts = path.split("/");
        int marketId = Integer.parseInt(parts[0]);
        String action = parts[1];

        if ("trip".equals(action)) {
            adminService.tripCircuitBreaker(marketId);
        } else if ("reset".equals(action)) {
            adminService.resetCircuitBreaker(marketId);
        } else {
            sendResponse(ctx, HttpResponseStatus.BAD_REQUEST, "{\"error\":\"Unknown action: " + action + "\"}");
            return;
        }

        ObjectNode resp = MAPPER.createObjectNode();
        resp.put("success", true);
        resp.put("marketId", marketId);
        resp.put("action", action);
        sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(resp));
    }

    private void sendResponse(ChannelHandlerContext ctx, HttpResponseStatus status, String json) {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        ByteBuf content = Unpooled.wrappedBuffer(bytes);
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    private static String escapeJson(String s) {
        if (s == null) return "null";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Unhandled exception in REST handler", cause);
        ctx.close();
    }
}
