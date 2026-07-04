package com.openexchange.oms.api.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.openexchange.oms.api.AdminService;
import com.openexchange.oms.api.OrderService;
import com.openexchange.oms.api.audit.AuditLog;
import com.openexchange.oms.api.auth.Authorizer;
import com.openexchange.oms.api.auth.HttpAuthHandler;
import com.openexchange.oms.api.auth.Principal;
import com.openexchange.oms.api.auth.RoleBasedAuthorizer;
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

    /** FixedPoint.fromDouble saturates around 9.2e10; reject sooner at the edge. */
    private static final double MAX_MONEY_INPUT = 9e10;

    private final OrderService orderService;
    private final AdminService adminService;
    private final Authorizer authorizer;
    private final CorsPolicy corsPolicy;
    private final AuditLog auditLog;

    // One request per connection (every response closes); set in channelRead0.
    private String requestOrigin;

    public RestApiHandler(OrderService orderService) {
        this(orderService, null, new RoleBasedAuthorizer(), CorsPolicy.fromSpec(""), AuditLog.disabled());
    }

    public RestApiHandler(OrderService orderService, AdminService adminService, Authorizer authorizer,
                          CorsPolicy corsPolicy, AuditLog auditLog) {
        this.orderService = orderService;
        this.adminService = adminService;
        this.authorizer = authorizer;
        this.corsPolicy = corsPolicy;
        this.auditLog = auditLog;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        String uri = request.uri();
        HttpMethod method = request.method();
        requestOrigin = request.headers().get(HttpHeaderNames.ORIGIN);

        // CORS preflight
        if (method == HttpMethod.OPTIONS) {
            sendCorsPreflightResponse(ctx);
            return;
        }

        try {
            if (uri.equals("/api/v1/health") && method == HttpMethod.GET) {
                handleHealth(ctx);
            } else if (uri.equals("/api/v1/orders") && method == HttpMethod.POST) {
                handleCreateOrder(ctx, request);
            } else if (uri.startsWith("/api/v1/orders/") && method == HttpMethod.DELETE) {
                handleCancelOrder(ctx, uri);
            } else if (uri.startsWith("/api/v1/orders/") && method == HttpMethod.PUT) {
                handleUpdateOrder(ctx, request, uri);
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
            } else if (uri.startsWith("/api/v1/admin/")) {
                Principal principal = principal(ctx);
                if (principal == null || !authorizer.allow(principal, Authorizer.ACTION_ADMIN, method + " " + uri)) {
                    sendResponse(ctx, HttpResponseStatus.FORBIDDEN, "{\"error\":\"Admin role required\"}");
                } else if (uri.startsWith("/api/v1/admin/risk/circuit-breaker/") && method == HttpMethod.POST) {
                    handleCircuitBreaker(ctx, uri);
                } else if (uri.startsWith("/api/v1/admin/risk/config/") && method == HttpMethod.PUT) {
                    handleUpdateRiskConfig(ctx, uri, request);
                } else if (uri.startsWith("/api/v1/admin/risk/config") && method == HttpMethod.GET) {
                    handleGetRiskConfig(ctx, uri);
                } else {
                    sendResponse(ctx, HttpResponseStatus.NOT_FOUND, "{\"error\":\"Not Found\"}");
                }
            } else {
                sendResponse(ctx, HttpResponseStatus.NOT_FOUND, "{\"error\":\"Not Found\"}");
            }
        } catch (Exception e) {
            log.error("Error handling request: {} {}", method, uri, e);
            sendResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
        }
    }

    private Principal principal(ChannelHandlerContext ctx) {
        return ctx.channel().attr(HttpAuthHandler.PRINCIPAL).get();
    }

    /** Whether the authenticated caller may act as the given user. */
    private boolean canActAs(ChannelHandlerContext ctx, long userId) {
        Principal p = principal(ctx);
        return p != null && authorizer.allow(p, Authorizer.ACTION_ACT_AS_USER, Long.toString(userId));
    }

    private void sendForbidden(ChannelHandlerContext ctx, long userId) {
        sendResponse(ctx, HttpResponseStatus.FORBIDDEN,
                "{\"error\":\"Forbidden: cannot act as user " + userId + "\"}");
    }

    /**
     * Ownership gate for order-id routes (cancel/update/get): responds 404 for
     * orders the caller may not touch — indistinguishable from a missing order,
     * so order ids can't be probed.
     */
    private boolean deniedOrderAccess(ChannelHandlerContext ctx, long omsOrderId) {
        OrderResponse existing = orderService.getOrder(omsOrderId);
        if (existing != null && !canActAs(ctx, existing.getUserId())) {
            sendResponse(ctx, HttpResponseStatus.NOT_FOUND, "{\"error\":\"Order not found\"}");
            return true;
        }
        return false;
    }

    /** Extract a numeric body field; null when absent or not a number. */
    private static Double num(Map<String, Object> body, String key) {
        Object value = body.get(key);
        return value instanceof Number n ? n.doubleValue() : null;
    }

    /** Positive, finite, and small enough for FixedPoint — money/quantity edge check. */
    private static boolean validMoney(double value) {
        return Double.isFinite(value) && value > 0 && value <= MAX_MONEY_INPUT;
    }

    /** Zero (= unset) or a valid money value — for optional numeric order fields. */
    private static boolean validOptionalMoney(double value) {
        return value == 0 || validMoney(value);
    }

    private void audit(ChannelHandlerContext ctx, String action, String resource, boolean success, String detail) {
        auditLog.record(principal(ctx), action, resource, success, detail);
    }

    private void handleHealth(ChannelHandlerContext ctx) throws Exception {
        ObjectNode health = MAPPER.createObjectNode();
        health.put("status", "ok");
        health.put("clusterConnected", orderService.isClusterConnected());
        health.put("activeOrders", orderService.getActiveOrderCount());
        sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(health));
    }

    private void handleCreateOrder(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        String body = request.content().toString(StandardCharsets.UTF_8);
        CreateOrderRequest req = MAPPER.readValue(body, CreateOrderRequest.class);

        // Identity comes from the principal; a caller-supplied userId is only
        // honored when the principal may act as that user (0 = unspecified).
        Principal p = principal(ctx);
        long userId = p == null ? req.getUserId() : p.resolveUserId(req.getUserId());
        if (!canActAs(ctx, userId)) {
            audit(ctx, "order.create", "user:" + userId, false, "forbidden");
            sendForbidden(ctx, userId);
            return;
        }
        req.setUserId(userId);

        // Edge validation (oms#37): risk checks ranges downstream, but malformed
        // input (JSON 1e999 = Infinity, overlong ids) must die at the edge.
        String invalid = validateCreateOrder(req);
        if (invalid != null) {
            audit(ctx, "order.create", "user:" + userId, false, invalid);
            sendResponse(ctx, HttpResponseStatus.BAD_REQUEST, "{\"error\":\"" + escapeJson(invalid) + "\"}");
            return;
        }

        CreateOrderResponse resp = orderService.createOrder(req);

        audit(ctx, "order.create", "user:" + userId, resp.isAccepted(),
                resp.isAccepted() ? "omsOrderId=" + resp.getOmsOrderId() : resp.getRejectReason());
        HttpResponseStatus status = resp.isAccepted()
            ? HttpResponseStatus.CREATED : HttpResponseStatus.BAD_REQUEST;
        sendResponse(ctx, status, MAPPER.writeValueAsString(resp));
    }

    private static String validateCreateOrder(CreateOrderRequest req) {
        if (req.getClientOrderId() != null && req.getClientOrderId().length() > 64) {
            return "clientOrderId too long (max 64)";
        }
        if (!validMoney(req.getQuantity())) return "quantity must be a positive finite number";
        if (!validOptionalMoney(req.getPrice())) return "price must be a positive finite number";
        if (!validOptionalMoney(req.getStopPrice())) return "stopPrice must be a positive finite number";
        if (!validOptionalMoney(req.getTrailingDelta())) return "trailingDelta must be a positive finite number";
        if (!validOptionalMoney(req.getDisplayQuantity())) return "displayQuantity must be a positive finite number";
        return null;
    }

    private void handleCancelOrder(ChannelHandlerContext ctx, String uri) throws Exception {
        String idStr = uri.substring("/api/v1/orders/".length());
        long omsOrderId = Long.parseLong(idStr);
        if (deniedOrderAccess(ctx, omsOrderId)) return;
        CancelOrderResponse resp = orderService.cancelOrder(omsOrderId);
        audit(ctx, "order.cancel", "order:" + omsOrderId, resp.isAccepted(), resp.getMessage());
        sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(resp));
    }

    private void handleUpdateOrder(ChannelHandlerContext ctx, FullHttpRequest request, String uri) throws Exception {
        String idStr = uri.substring("/api/v1/orders/".length());
        long omsOrderId = Long.parseLong(idStr);
        if (deniedOrderAccess(ctx, omsOrderId)) return;
        String body = request.content().toString(StandardCharsets.UTF_8);
        Map<String, Object> params = MAPPER.readValue(body, Map.class);

        Double priceParam = num(params, "price");
        Double quantityParam = num(params, "quantity");
        double newPrice = priceParam != null ? priceParam : 0;
        double newQuantity = quantityParam != null ? quantityParam : 0;

        if (newPrice <= 0 && newQuantity <= 0) {
            sendResponse(ctx, HttpResponseStatus.BAD_REQUEST,
                    "{\"error\":\"At least one of price or quantity must be provided\"}");
            return;
        }
        if (!validOptionalMoney(newPrice) || !validOptionalMoney(newQuantity)) {
            sendResponse(ctx, HttpResponseStatus.BAD_REQUEST,
                    "{\"error\":\"price/quantity must be positive finite numbers\"}");
            return;
        }

        Map<String, Object> result = orderService.updateOrder(omsOrderId, newPrice, newQuantity);
        boolean accepted = Boolean.TRUE.equals(result.get("accepted"));
        audit(ctx, "order.update", "order:" + omsOrderId, accepted,
                "price=" + newPrice + " quantity=" + newQuantity);
        HttpResponseStatus status = accepted ? HttpResponseStatus.OK : HttpResponseStatus.BAD_REQUEST;
        sendResponse(ctx, status, MAPPER.writeValueAsString(result));
    }

    private void handleGetOrder(ChannelHandlerContext ctx, String uri) throws Exception {
        String idStr = uri.substring("/api/v1/orders/".length());
        long omsOrderId = Long.parseLong(idStr);
        OrderResponse resp = orderService.getOrder(omsOrderId);
        if (resp == null || !canActAs(ctx, resp.getUserId())) {
            sendResponse(ctx, HttpResponseStatus.NOT_FOUND, "{\"error\":\"Order not found\"}");
        } else {
            sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(resp));
        }
    }

    private void handleQueryOrders(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        String userIdStr = decoder.parameters().containsKey("userId")
            ? decoder.parameters().get("userId").getFirst() : null;
        String statusStr = decoder.parameters().containsKey("status")
            ? decoder.parameters().get("status").getFirst() : null;

        Principal p = principal(ctx);
        if (userIdStr == null && p == null) {
            sendResponse(ctx, HttpResponseStatus.BAD_REQUEST, "{\"error\":\"userId required\"}");
            return;
        }

        long userId = userIdStr != null ? Long.parseLong(userIdStr) : p.userId();
        if (!canActAs(ctx, userId)) {
            sendForbidden(ctx, userId);
            return;
        }
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
        if (!canActAs(ctx, userId)) {
            sendForbidden(ctx, userId);
            return;
        }
        var balances = orderService.getBalances(userId);
        sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(balances));
    }

    @SuppressWarnings("unchecked")
    private void handleDeposit(ChannelHandlerContext ctx, String uri, FullHttpRequest request) throws Exception {
        long userId = extractAccountUserId(uri);
        if (!canActAs(ctx, userId)) {
            sendForbidden(ctx, userId);
            return;
        }
        String body = request.content().toString(StandardCharsets.UTF_8);
        Map<String, Object> req = MAPPER.readValue(body, Map.class);

        Double assetIdParam = num(req, "assetId");
        Double amountParam = num(req, "amount");
        if (assetIdParam == null || amountParam == null || !validMoney(amountParam)
                || assetIdParam != Math.floor(assetIdParam) || assetIdParam < 1 || assetIdParam > 1_000_000) {
            audit(ctx, "account.deposit", "user:" + userId, false, "invalid input");
            sendResponse(ctx, HttpResponseStatus.BAD_REQUEST,
                    "{\"error\":\"assetId (positive integer) and amount (positive finite number) required\"}");
            return;
        }
        int assetId = assetIdParam.intValue();
        double amount = amountParam;
        long fpAmount = com.match.domain.FixedPoint.fromDouble(amount);

        orderService.deposit(userId, assetId, fpAmount);
        audit(ctx, "account.deposit", "user:" + userId, true, "assetId=" + assetId + " amount=" + amount);

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
        if (!canActAs(ctx, userId)) {
            sendForbidden(ctx, userId);
            return;
        }
        String body = request.content().toString(StandardCharsets.UTF_8);
        Map<String, Object> req = MAPPER.readValue(body, Map.class);

        Double assetIdParam = num(req, "assetId");
        Double amountParam = num(req, "amount");
        if (assetIdParam == null || amountParam == null || !validMoney(amountParam)
                || assetIdParam != Math.floor(assetIdParam) || assetIdParam < 1 || assetIdParam > 1_000_000) {
            audit(ctx, "account.withdraw", "user:" + userId, false, "invalid input");
            sendResponse(ctx, HttpResponseStatus.BAD_REQUEST,
                    "{\"error\":\"assetId (positive integer) and amount (positive finite number) required\"}");
            return;
        }
        int assetId = assetIdParam.intValue();
        double amount = amountParam;
        long fpAmount = com.match.domain.FixedPoint.fromDouble(amount);

        try {
            orderService.withdraw(userId, assetId, fpAmount);
            audit(ctx, "account.withdraw", "user:" + userId, true, "assetId=" + assetId + " amount=" + amount);
            ObjectNode resp = MAPPER.createObjectNode();
            resp.put("success", true);
            resp.put("userId", userId);
            resp.put("assetId", assetId);
            resp.put("amount", amount);
            sendResponse(ctx, HttpResponseStatus.OK, MAPPER.writeValueAsString(resp));
        } catch (IllegalStateException e) {
            audit(ctx, "account.withdraw", "user:" + userId, false, e.getMessage());
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
        audit(ctx, "admin.risk.update", "market:" + marketId, true, String.valueOf(fields.keySet()));

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
            sendResponse(ctx, HttpResponseStatus.BAD_REQUEST, "{\"error\":\"Unknown action: " + escapeJson(action) + "\"}");
            return;
        }
        audit(ctx, "admin.circuit-breaker", "market:" + marketId, true, action);

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
        corsPolicy.apply(requestOrigin, response);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    private void sendCorsPreflightResponse(ChannelHandlerContext ctx) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);
        corsPolicy.apply(requestOrigin, response);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
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
