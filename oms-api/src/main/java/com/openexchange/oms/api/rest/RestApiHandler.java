// SPDX-License-Identifier: Apache-2.0
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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
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
 *
 * <p>Keep-alive (oms#66): every response honors the client's Connection
 * preference via {@link HttpUtil#setKeepAlive}. HTTP/1.1 clients reuse the
 * connection; the socket is closed only when the caller asked to (or spoke
 * HTTP/1.0 without keep-alive). Because one handler instance now serves many
 * requests, per-request state (the CORS Origin, the keep-alive flag) travels
 * in a {@link RequestContext} rather than an instance field — the async auth
 * path (see {@link #runAuthRequest}) responds later, after a subsequent
 * request on the same connection may already have arrived.
 */
public class RestApiHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private static final Logger log = LoggerFactory.getLogger(RestApiHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Money bound: 9e10 units in 8-dp fixed-point; the representable ceiling is ~9.22e10. */
    private static final long MAX_MONEY_FP = 9_000_000_000_000_000_000L;

    // Machine-readable error codes — the frozen error contract (oms#39):
    // every non-2xx body is {"error": <human text>, "code": <one of these>}.
    // Business rejections additionally carry rejectReason (RiskRejectReason).
    static final String ERR_VALIDATION = "VALIDATION";
    static final String ERR_UNAUTHORIZED = "UNAUTHORIZED";
    static final String ERR_FORBIDDEN = "FORBIDDEN";
    static final String ERR_NOT_FOUND = "NOT_FOUND";
    static final String ERR_REJECTED = "REJECTED";
    static final String ERR_ADMIN_UNAVAILABLE = "ADMIN_UNAVAILABLE";
    static final String ERR_UNAVAILABLE = "UNAVAILABLE";
    static final String ERR_INTERNAL = "INTERNAL";

    private final OrderService orderService;
    private final AdminService adminService;
    private final Authorizer authorizer;
    private final CorsPolicy corsPolicy;
    private final AuditLog auditLog;
    private final PrometheusMeterRegistry meterRegistry;

    // Demo auth (OMS_AUTH_MODE=demo); null in other modes -> auth routes 503.
    private volatile com.openexchange.oms.api.auth.AuthService authService;

    // Register/login run PBKDF2 + JDBC — never on the Netty event loop.
    // Request bodies are extracted to Strings before dispatch (the request
    // buffer is auto-released when channelRead0 returns).
    private static final java.util.concurrent.ExecutorService AUTH_EXECUTOR =
            java.util.concurrent.Executors.newFixedThreadPool(2, r -> {
                Thread t = new Thread(r, "oms-auth");
                t.setDaemon(true);
                return t;
            });

    /**
     * Per-request state carried through the response writers. Under keep-alive
     * one handler instance serves many requests, so the Origin (for CORS) and
     * the keep-alive flag cannot live on the handler: the async auth path
     * responds after a later request may have overwritten a shared field
     * (oms#66 HAZARD 1). Captured once in {@link #channelRead0}; {@code origin}
     * is already a String, so it stays valid after the request buffer is freed.
     */
    private record RequestContext(ChannelHandlerContext ctx, String origin, boolean keepAlive) {}

    public RestApiHandler(OrderService orderService) {
        this(orderService, null, new RoleBasedAuthorizer(), CorsPolicy.fromSpec(""), AuditLog.disabled(),
                new PrometheusMeterRegistry(io.micrometer.prometheusmetrics.PrometheusConfig.DEFAULT));
    }

    public RestApiHandler(OrderService orderService, AdminService adminService, Authorizer authorizer,
                          CorsPolicy corsPolicy, AuditLog auditLog, PrometheusMeterRegistry meterRegistry) {
        this.orderService = orderService;
        this.adminService = adminService;
        this.authorizer = authorizer;
        this.corsPolicy = corsPolicy;
        this.auditLog = auditLog;
        this.meterRegistry = meterRegistry;
    }

    /** Demo auth service (OMS_AUTH_MODE=demo). Absent -> /api/v1/auth/* returns 503. */
    public void setAuthService(com.openexchange.oms.api.auth.AuthService authService) {
        this.authService = authService;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        String uri = request.uri();
        HttpMethod method = request.method();
        // Capture per-request state before any async hop (oms#66 HAZARD 1).
        RequestContext rc = new RequestContext(ctx,
                request.headers().get(HttpHeaderNames.ORIGIN),
                HttpUtil.isKeepAlive(request));

        // CORS preflight
        if (method == HttpMethod.OPTIONS) {
            sendCorsPreflightResponse(rc);
            return;
        }

        // Request timing (oms#38): handlers are synchronous, so wall time here
        // is the request latency. Route labels are a fixed set (no id parts).
        final long startNanos = System.nanoTime();
        try {
            route(rc, request, uri, method);
        } finally {
            Timer.builder("oms_http_request_seconds")
                    .description("OMS REST request latency")
                    .tag("route", routeLabel(uri, method))
                    .publishPercentiles(0.5, 0.95, 0.99)
                    .register(meterRegistry)
                    .record(System.nanoTime() - startNanos, java.util.concurrent.TimeUnit.NANOSECONDS);
        }
    }

    private void route(RequestContext rc, FullHttpRequest request, String uri, HttpMethod method) {
        try {
            if (uri.equals("/metrics") && method == HttpMethod.GET) {
                handleMetrics(rc);
            } else if (uri.equals("/api/v1/health") && method == HttpMethod.GET) {
                handleHealth(rc);
            } else if (uri.equals("/api/v1/auth/register") && method == HttpMethod.POST) {
                handleRegister(rc, request);
            } else if (uri.equals("/api/v1/auth/login") && method == HttpMethod.POST) {
                handleLogin(rc, request);
            } else if (uri.equals("/api/v1/auth/me") && method == HttpMethod.GET) {
                handleMe(rc);
            } else if (uri.equals("/api/v1/orders") && method == HttpMethod.POST) {
                handleCreateOrder(rc, request);
            } else if (stripQuery(uri).equals("/api/v1/orders/history") && method == HttpMethod.GET) {
                handleOrderHistory(rc, request);
            } else if (stripQuery(uri).equals("/api/v1/executions") && method == HttpMethod.GET) {
                handleExecutions(rc, request);
            } else if (stripQuery(uri).equals("/api/v1/positions") && method == HttpMethod.GET) {
                handlePositions(rc, request);
            } else if (uri.startsWith("/api/v1/orders/") && method == HttpMethod.DELETE) {
                handleCancelOrder(rc, uri);
            } else if (uri.startsWith("/api/v1/orders/") && method == HttpMethod.PUT) {
                handleUpdateOrder(rc, request, uri);
            } else if (uri.startsWith("/api/v1/orders/") && method == HttpMethod.GET) {
                handleGetOrder(rc, uri);
            } else if (uri.startsWith("/api/v1/orders") && method == HttpMethod.GET) {
                handleQueryOrders(rc, request);
            } else if (uri.matches("/api/v1/accounts/\\d+/deposit") && method == HttpMethod.POST) {
                handleDeposit(rc, uri, request);
            } else if (uri.matches("/api/v1/accounts/\\d+/withdraw") && method == HttpMethod.POST) {
                handleWithdraw(rc, uri, request);
            } else if (uri.startsWith("/api/v1/accounts/") && method == HttpMethod.GET) {
                handleGetAccount(rc, uri);
            } else if (uri.equals("/api/v1/markets") && method == HttpMethod.GET) {
                handleGetMarkets(rc);
            } else if (uri.startsWith("/api/v1/admin/")) {
                Principal principal = principal(rc);
                if (principal == null || !authorizer.allow(principal, Authorizer.ACTION_ADMIN, method + " " + uri)) {
                    sendError(rc, HttpResponseStatus.FORBIDDEN, ERR_FORBIDDEN, "Admin role required");
                } else if (uri.startsWith("/api/v1/admin/risk/circuit-breaker/") && method == HttpMethod.POST) {
                    handleCircuitBreaker(rc, uri);
                } else if (uri.startsWith("/api/v1/admin/risk/config/") && method == HttpMethod.PUT) {
                    handleUpdateRiskConfig(rc, uri, request);
                } else if (uri.startsWith("/api/v1/admin/risk/config") && method == HttpMethod.GET) {
                    handleGetRiskConfig(rc, uri);
                } else {
                    sendError(rc, HttpResponseStatus.NOT_FOUND, ERR_NOT_FOUND, "Not Found");
                }
            } else {
                sendError(rc, HttpResponseStatus.NOT_FOUND, ERR_NOT_FOUND, "Not Found");
            }
        } catch (Exception e) {
            log.error("Error handling request: {} {}", method, uri, e);
            sendError(rc, HttpResponseStatus.INTERNAL_SERVER_ERROR, ERR_INTERNAL, e.getMessage());
        }
    }

    private static String stripQuery(String uri) {
        int q = uri.indexOf('?');
        return q >= 0 ? uri.substring(0, q) : uri;
    }

    /** Fixed-cardinality route label: id/query parts stripped. */
    private static String routeLabel(String uri, HttpMethod method) {
        String path = stripQuery(uri);
        String m = method.name();
        if (path.equals("/metrics")) return "metrics";
        if (path.equals("/api/v1/health")) return "health";
        if (path.equals("/api/v1/auth/register")) return "auth.register";
        if (path.equals("/api/v1/auth/login")) return "auth.login";
        if (path.equals("/api/v1/auth/me")) return "auth.me";
        if (path.equals("/api/v1/markets")) return "markets";
        if (path.equals("/api/v1/orders")) return m.equals("POST") ? "orders.create" : "orders.query";
        if (path.equals("/api/v1/orders/history")) return "orders.history";
        if (path.equals("/api/v1/executions")) return "executions";
        if (path.equals("/api/v1/positions")) return "positions";
        if (path.startsWith("/api/v1/orders/")) {
            return switch (m) {
                case "DELETE" -> "orders.cancel";
                case "PUT" -> "orders.update";
                default -> "orders.get";
            };
        }
        if (path.startsWith("/api/v1/accounts/")) {
            if (path.endsWith("/deposit")) return "accounts.deposit";
            if (path.endsWith("/withdraw")) return "accounts.withdraw";
            return "accounts.get";
        }
        if (path.startsWith("/api/v1/admin/")) return "admin";
        return "other";
    }

    private void handleMetrics(RequestContext rc) {
        String scrape = meterRegistry.scrape();
        byte[] bytes = scrape.getBytes(StandardCharsets.UTF_8);
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(bytes));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        finishResponse(rc, response);
    }

    private Principal principal(RequestContext rc) {
        return rc.ctx().channel().attr(HttpAuthHandler.PRINCIPAL).get();
    }

    /** Whether the authenticated caller may act as the given user. */
    private boolean canActAs(RequestContext rc, long userId) {
        Principal p = principal(rc);
        return p != null && authorizer.allow(p, Authorizer.ACTION_ACT_AS_USER, Long.toString(userId));
    }

    // ==================== Demo auth (OMS_AUTH_MODE=demo) ====================

    private void handleRegister(RequestContext rc, FullHttpRequest request) {
        runAuthRequest(rc, request, (username, password) -> {
            com.openexchange.oms.api.auth.AuthService.Session session =
                    authService.register(username, password);
            log.info("Registered demo user '{}' (userId={})", session.username(), session.userId());
            return session;
        });
    }

    private void handleLogin(RequestContext rc, FullHttpRequest request) {
        runAuthRequest(rc, request, (username, password) -> authService.login(username, password));
    }

    /** Token echo — lets the UI validate a stored session on load. */
    private void handleMe(RequestContext rc) {
        Principal p = principal(rc);
        if (p == null) {
            sendError(rc, HttpResponseStatus.UNAUTHORIZED, ERR_UNAUTHORIZED, "Unauthorized");
            return;
        }
        String subject = p.subject();
        String username = subject.startsWith("user-") ? subject.substring(5) : subject;
        sendResponse(rc, HttpResponseStatus.OK,
                "{\"userId\":" + p.userId() + ",\"username\":\"" + escapeJson(username) + "\"}");
    }

    private interface AuthCall {
        com.openexchange.oms.api.auth.AuthService.Session run(String username, String password);
    }

    /**
     * Shared register/login plumbing: body parsed on the event loop (the
     * request buffer is released when channelRead0 returns), credential work
     * on AUTH_EXECUTOR, null session = bad credentials. The RequestContext is
     * captured per request, so the deferred response applies the right CORS
     * Origin / keep-alive even if a newer request has since arrived (oms#66).
     */
    private void runAuthRequest(RequestContext rc, FullHttpRequest request, AuthCall call) {
        if (authService == null) {
            sendError(rc, HttpResponseStatus.SERVICE_UNAVAILABLE, ERR_UNAVAILABLE,
                    "User accounts unavailable: OMS is not running in demo auth mode with persistence");
            return;
        }
        final String body = request.content().toString(StandardCharsets.UTF_8);
        AUTH_EXECUTOR.execute(() -> {
            try {
                com.fasterxml.jackson.databind.JsonNode json = MAPPER.readTree(body);
                String username = json.path("username").asText(null);
                String password = json.path("password").asText(null);
                com.openexchange.oms.api.auth.AuthService.Session session = call.run(username, password);
                if (session == null) {
                    sendError(rc, HttpResponseStatus.UNAUTHORIZED, ERR_UNAUTHORIZED,
                            "Invalid username or password");
                    return;
                }
                sendResponse(rc, HttpResponseStatus.OK, MAPPER.writeValueAsString(session));
            } catch (com.openexchange.oms.api.auth.AuthService.UsernameTakenException e) {
                sendError(rc, HttpResponseStatus.CONFLICT, ERR_VALIDATION, e.getMessage());
            } catch (IllegalArgumentException e) {
                sendError(rc, HttpResponseStatus.BAD_REQUEST, ERR_VALIDATION, e.getMessage());
            } catch (IllegalStateException e) {
                sendError(rc, HttpResponseStatus.SERVICE_UNAVAILABLE, ERR_UNAVAILABLE, e.getMessage());
            } catch (Exception e) {
                log.error("Auth request failed", e);
                sendError(rc, HttpResponseStatus.INTERNAL_SERVER_ERROR, ERR_INTERNAL, rootMessage(e));
            }
        });
    }

    private void sendForbidden(RequestContext rc, long userId) {
        sendError(rc, HttpResponseStatus.FORBIDDEN, ERR_FORBIDDEN, "Forbidden: cannot act as user " + userId);
    }

    /**
     * Ownership gate for order-id routes (cancel/update/get): responds 404 for
     * orders the caller may not touch — indistinguishable from a missing order,
     * so order ids can't be probed.
     */
    private boolean deniedOrderAccess(RequestContext rc, long omsOrderId) {
        OrderResponse existing = orderService.getOrder(omsOrderId);
        if (existing != null && !canActAs(rc, existing.getUserId())) {
            sendError(rc, HttpResponseStatus.NOT_FOUND, ERR_NOT_FOUND, "Order not found");
            return true;
        }
        return false;
    }

    /**
     * Extract a money field from a JSON body as 8-dp fixed-point (oms#39):
     * decimal STRING = canonical, exact; JSON number = deprecated legacy path
     * (rounded via the double). Returns null when absent or JSON null.
     *
     * @throws IllegalArgumentException on a malformed or out-of-range value
     */
    private static Long money(com.fasterxml.jackson.databind.JsonNode body, String key) {
        com.fasterxml.jackson.databind.JsonNode node = body.get(key);
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isTextual()) {
            try {
                return com.match.domain.FixedPoint.parse(node.textValue());
            } catch (NumberFormatException | com.match.domain.FixedPoint.OverflowException e) {
                throw new IllegalArgumentException(key + " must be a decimal string with at most 8 fractional digits");
            }
        }
        if (node.isNumber()) {
            double d = node.doubleValue();
            if (!Double.isFinite(d)) {
                throw new IllegalArgumentException(key + " must be finite");
            }
            return com.match.domain.FixedPoint.fromDouble(d);
        }
        throw new IllegalArgumentException(key + " must be a decimal string");
    }

    /** Positive and inside the money bound — fixed-point edge check. */
    private static boolean validMoney(long value) {
        return value > 0 && value <= MAX_MONEY_FP;
    }

    /** Zero (= unset) or a valid money value — for optional order fields. */
    private static boolean validOptionalMoney(long value) {
        return value == 0 || validMoney(value);
    }

    private void audit(RequestContext rc, String action, String resource, boolean success, String detail) {
        auditLog.record(principal(rc), action, resource, success, detail);
    }

    private void handleHealth(RequestContext rc) throws Exception {
        ObjectNode health = MAPPER.createObjectNode();
        health.put("status", "ok");
        health.put("clusterConnected", orderService.isClusterConnected());
        health.put("activeOrders", orderService.getActiveOrderCount());
        // E3/E4: only present when an AE-backed balance store is active (null otherwise).
        Boolean assetsProjectionReady = orderService.isAssetsProjectionReady();
        if (assetsProjectionReady != null) {
            health.put("assetsProjectionReady", assetsProjectionReady);
        }
        sendResponse(rc, HttpResponseStatus.OK, MAPPER.writeValueAsString(health));
    }

    private void handleCreateOrder(RequestContext rc, FullHttpRequest request) throws Exception {
        String body = request.content().toString(StandardCharsets.UTF_8);
        CreateOrderRequest req;
        try {
            req = MAPPER.readValue(body, CreateOrderRequest.class);
        } catch (java.io.IOException e) {
            // Malformed JSON or a bad money string from FixedPointJson: caller error
            sendError(rc, HttpResponseStatus.BAD_REQUEST, ERR_VALIDATION, rootMessage(e));
            return;
        }

        // Identity comes from the principal; a caller-supplied userId is only
        // honored when the principal may act as that user (0 = unspecified).
        Principal p = principal(rc);
        long userId = p == null ? req.getUserId() : p.resolveUserId(req.getUserId());
        if (!canActAs(rc, userId)) {
            audit(rc, "order.create", "user:" + userId, false, "forbidden");
            sendForbidden(rc, userId);
            return;
        }
        req.setUserId(userId);

        // Edge validation (oms#37): risk checks ranges downstream, but malformed
        // input (JSON 1e999 = Infinity, overlong ids) must die at the edge.
        String invalid = validateCreateOrder(req);
        if (invalid != null) {
            audit(rc, "order.create", "user:" + userId, false, invalid);
            sendError(rc, HttpResponseStatus.BAD_REQUEST, ERR_VALIDATION, invalid);
            return;
        }

        CreateOrderResponse resp = orderService.createOrder(req);

        // Reject reasons are a small fixed set (risk enum + admission strings).
        Counter.builder("oms_orders_total")
                .description("Order submissions by outcome")
                .tag("result", resp.isDuplicate() ? "duplicate" : resp.isAccepted() ? "accepted" : "rejected")
                .tag("reason", resp.isAccepted() ? "none"
                        : (resp.getRejectReason() != null ? resp.getRejectReason().replace(' ', '_') : "unknown"))
                .register(meterRegistry)
                .increment();

        audit(rc, "order.create", "user:" + userId, resp.isAccepted(),
                resp.isAccepted()
                        ? "omsOrderId=" + resp.getOmsOrderId() + (resp.isDuplicate() ? " duplicate" : "")
                        : resp.getRejectReason());
        // Duplicate clientOrderId replays the existing order: 200, not 201 (oms#40)
        HttpResponseStatus status = !resp.isAccepted() ? HttpResponseStatus.BAD_REQUEST
                : resp.isDuplicate() ? HttpResponseStatus.OK : HttpResponseStatus.CREATED;
        sendResponse(rc, status, MAPPER.writeValueAsString(resp));
    }

    private static String validateCreateOrder(CreateOrderRequest req) {
        if (req.getClientOrderId() != null && req.getClientOrderId().length() > 64) {
            return "clientOrderId too long (max 64)";
        }
        if (!validMoney(req.getQuantity())) return "quantity must be a positive decimal";
        if (!validOptionalMoney(req.getPrice())) return "price must be a positive decimal";
        if (!validOptionalMoney(req.getStopPrice())) return "stopPrice must be a positive decimal";
        if (!validOptionalMoney(req.getTrailingDelta())) return "trailingDelta must be a positive decimal";
        if (!validOptionalMoney(req.getDisplayQuantity())) return "displayQuantity must be a positive decimal";
        return null;
    }

    private void handleCancelOrder(RequestContext rc, String uri) throws Exception {
        String idStr = uri.substring("/api/v1/orders/".length());
        long omsOrderId = Long.parseLong(idStr);
        if (deniedOrderAccess(rc, omsOrderId)) return;
        CancelOrderResponse resp = orderService.cancelOrder(omsOrderId);
        audit(rc, "order.cancel", "order:" + omsOrderId, resp.isAccepted(), resp.getMessage());
        sendResponse(rc, HttpResponseStatus.OK, MAPPER.writeValueAsString(resp));
    }

    private void handleUpdateOrder(RequestContext rc, FullHttpRequest request, String uri) throws Exception {
        String idStr = uri.substring("/api/v1/orders/".length());
        long omsOrderId = Long.parseLong(idStr);
        if (deniedOrderAccess(rc, omsOrderId)) return;
        String body = request.content().toString(StandardCharsets.UTF_8);
        long newPrice;
        long newQuantity;
        try {
            com.fasterxml.jackson.databind.JsonNode params = MAPPER.readTree(body);
            Long priceParam = money(params, "price");
            Long quantityParam = money(params, "quantity");
            newPrice = priceParam != null ? priceParam : 0;
            newQuantity = quantityParam != null ? quantityParam : 0;
        } catch (IllegalArgumentException | java.io.IOException e) {
            sendError(rc, HttpResponseStatus.BAD_REQUEST, ERR_VALIDATION, rootMessage(e));
            return;
        }

        if (newPrice <= 0 && newQuantity <= 0) {
            sendError(rc, HttpResponseStatus.BAD_REQUEST, ERR_VALIDATION,
                    "At least one of price or quantity must be provided");
            return;
        }
        if (!validOptionalMoney(newPrice) || !validOptionalMoney(newQuantity)) {
            sendError(rc, HttpResponseStatus.BAD_REQUEST, ERR_VALIDATION,
                    "price/quantity must be positive decimals");
            return;
        }

        Map<String, Object> result = orderService.updateOrder(omsOrderId, newPrice, newQuantity);
        boolean accepted = Boolean.TRUE.equals(result.get("accepted"));
        audit(rc, "order.update", "order:" + omsOrderId, accepted,
                "price=" + com.match.domain.FixedPoint.format(newPrice)
                + " quantity=" + com.match.domain.FixedPoint.format(newQuantity));
        HttpResponseStatus status = accepted ? HttpResponseStatus.OK : HttpResponseStatus.BAD_REQUEST;
        sendResponse(rc, status, MAPPER.writeValueAsString(result));
    }

    private void handleGetOrder(RequestContext rc, String uri) throws Exception {
        String idStr = uri.substring("/api/v1/orders/".length());
        long omsOrderId = Long.parseLong(idStr);
        OrderResponse resp = orderService.getOrder(omsOrderId);
        if (resp == null || !canActAs(rc, resp.getUserId())) {
            sendError(rc, HttpResponseStatus.NOT_FOUND, ERR_NOT_FOUND, "Order not found");
        } else {
            sendResponse(rc, HttpResponseStatus.OK, MAPPER.writeValueAsString(resp));
        }
    }

    private void handleQueryOrders(RequestContext rc, FullHttpRequest request) throws Exception {
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        Long userId = resolveQueryUserId(rc, decoder);
        if (userId == null) return;
        var orders = orderService.queryOrders(userId, firstParam(decoder, "status"));
        sendResponse(rc, HttpResponseStatus.OK, MAPPER.writeValueAsString(orders));
    }

    /**
     * History reads (oms#40) — Postgres-backed, so they survive OMS restarts.
     * All three take the same identity/paging query params; 503 UNAVAILABLE
     * when the OMS runs without persistence.
     */
    private void handleOrderHistory(RequestContext rc, FullHttpRequest request) throws Exception {
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        Long userId = resolveQueryUserId(rc, decoder);
        if (userId == null) return;
        String status = firstParam(decoder, "status");
        Integer limit = intParam(rc, decoder, "limit", 100);
        Integer offset = limit != null ? intParam(rc, decoder, "offset", 0) : null;
        if (offset == null) return;
        try {
            var orders = orderService.getOrderHistory(userId, status, limit, offset);
            sendResponse(rc, HttpResponseStatus.OK, MAPPER.writeValueAsString(orders));
        } catch (IllegalArgumentException e) {
            sendError(rc, HttpResponseStatus.BAD_REQUEST, ERR_VALIDATION, "Unknown status: " + status);
        } catch (IllegalStateException e) {
            sendError(rc, HttpResponseStatus.SERVICE_UNAVAILABLE, ERR_UNAVAILABLE, e.getMessage());
        }
    }

    private void handleExecutions(RequestContext rc, FullHttpRequest request) throws Exception {
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        Long userId = resolveQueryUserId(rc, decoder);
        if (userId == null) return;
        Integer limit = intParam(rc, decoder, "limit", 100);
        Integer offset = limit != null ? intParam(rc, decoder, "offset", 0) : null;
        if (offset == null) return;
        try {
            var executions = orderService.getExecutions(userId, limit, offset);
            sendResponse(rc, HttpResponseStatus.OK, MAPPER.writeValueAsString(executions));
        } catch (IllegalStateException e) {
            sendError(rc, HttpResponseStatus.SERVICE_UNAVAILABLE, ERR_UNAVAILABLE, e.getMessage());
        }
    }

    private void handlePositions(RequestContext rc, FullHttpRequest request) throws Exception {
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        Long userId = resolveQueryUserId(rc, decoder);
        if (userId == null) return;
        try {
            var positions = orderService.getPositions(userId);
            sendResponse(rc, HttpResponseStatus.OK, MAPPER.writeValueAsString(positions));
        } catch (IllegalStateException e) {
            sendError(rc, HttpResponseStatus.SERVICE_UNAVAILABLE, ERR_UNAVAILABLE, e.getMessage());
        }
    }

    /**
     * Identity for user-scoped query endpoints: explicit ?userId= when the
     * principal may act as that user, else the principal's own. Sends the
     * error and returns null when unresolvable/forbidden.
     */
    private Long resolveQueryUserId(RequestContext rc, QueryStringDecoder decoder) {
        String userIdStr = firstParam(decoder, "userId");
        Principal p = principal(rc);
        if (userIdStr == null && p == null) {
            sendError(rc, HttpResponseStatus.BAD_REQUEST, ERR_VALIDATION, "userId required");
            return null;
        }
        long userId;
        try {
            userId = userIdStr != null ? Long.parseLong(userIdStr) : p.userId();
        } catch (NumberFormatException e) {
            sendError(rc, HttpResponseStatus.BAD_REQUEST, ERR_VALIDATION, "userId must be an integer");
            return null;
        }
        if (!canActAs(rc, userId)) {
            sendForbidden(rc, userId);
            return null;
        }
        return userId;
    }

    private static String firstParam(QueryStringDecoder decoder, String name) {
        var values = decoder.parameters().get(name);
        return values != null && !values.isEmpty() ? values.getFirst() : null;
    }

    /** Non-negative int query param; sends 400 and returns null when malformed. */
    private Integer intParam(RequestContext rc, QueryStringDecoder decoder, String name, int defaultValue) {
        String raw = firstParam(decoder, name);
        if (raw == null) return defaultValue;
        try {
            int value = Integer.parseInt(raw);
            if (value < 0) throw new NumberFormatException();
            return value;
        } catch (NumberFormatException e) {
            sendError(rc, HttpResponseStatus.BAD_REQUEST, ERR_VALIDATION, name + " must be a non-negative integer");
            return null;
        }
    }

    private void handleGetAccount(RequestContext rc, String uri) throws Exception {
        String idStr = uri.substring("/api/v1/accounts/".length());
        int slash = idStr.indexOf('/');
        if (slash > 0) {
            idStr = idStr.substring(0, slash);
        }
        long userId = Long.parseLong(idStr);
        if (!canActAs(rc, userId)) {
            sendForbidden(rc, userId);
            return;
        }
        var balances = orderService.getBalances(userId);
        sendResponse(rc, HttpResponseStatus.OK, MAPPER.writeValueAsString(balances));
    }

    private void handleDeposit(RequestContext rc, String uri, FullHttpRequest request) throws Exception {
        long userId = extractAccountUserId(uri);
        if (!canActAs(rc, userId)) {
            sendForbidden(rc, userId);
            return;
        }
        long[] parsed = parseAssetAmount(rc, request, "account.deposit", userId);
        if (parsed == null) {
            return;
        }
        int assetId = (int) parsed[0];
        long fpAmount = parsed[1];

        orderService.deposit(userId, assetId, fpAmount);
        String amountStr = com.match.domain.FixedPoint.format(fpAmount);
        audit(rc, "account.deposit", "user:" + userId, true, "assetId=" + assetId + " amount=" + amountStr);
        sendResponse(rc, HttpResponseStatus.OK,
                MAPPER.writeValueAsString(moneyMovementResponse(userId, assetId, amountStr)));
    }

    private void handleWithdraw(RequestContext rc, String uri, FullHttpRequest request) throws Exception {
        long userId = extractAccountUserId(uri);
        if (!canActAs(rc, userId)) {
            sendForbidden(rc, userId);
            return;
        }
        long[] parsed = parseAssetAmount(rc, request, "account.withdraw", userId);
        if (parsed == null) {
            return;
        }
        int assetId = (int) parsed[0];
        long fpAmount = parsed[1];
        String amountStr = com.match.domain.FixedPoint.format(fpAmount);

        try {
            orderService.withdraw(userId, assetId, fpAmount);
            audit(rc, "account.withdraw", "user:" + userId, true, "assetId=" + assetId + " amount=" + amountStr);
            sendResponse(rc, HttpResponseStatus.OK,
                    MAPPER.writeValueAsString(moneyMovementResponse(userId, assetId, amountStr)));
        } catch (IllegalStateException e) {
            audit(rc, "account.withdraw", "user:" + userId, false, e.getMessage());
            sendError(rc, HttpResponseStatus.BAD_REQUEST, ERR_REJECTED, e.getMessage());
        }
    }

    /**
     * Shared deposit/withdraw body parsing: {"assetId": int, "amount": money}.
     * Amount follows the money() rules (decimal string canonical, number
     * deprecated). Returns {assetId, fixedPointAmount}, or null after having
     * sent the 400.
     */
    private long[] parseAssetAmount(RequestContext rc, FullHttpRequest request,
                                    String auditAction, long userId) throws Exception {
        String body = request.content().toString(StandardCharsets.UTF_8);
        try {
            com.fasterxml.jackson.databind.JsonNode req = MAPPER.readTree(body);
            com.fasterxml.jackson.databind.JsonNode assetNode = req.get("assetId");
            Long amount = money(req, "amount");
            // assetId 0 is USD — the oms#37 lower bound of 1 broke USD deposits
            // (harness seeding included); ids are 0-based.
            if (assetNode == null || !assetNode.isNumber() || !assetNode.canConvertToExactIntegral()
                    || assetNode.asInt() < 0 || assetNode.asInt() > 1_000_000
                    || amount == null || !validMoney(amount)) {
                throw new IllegalArgumentException(
                        "assetId (non-negative integer) and amount (positive decimal string) required");
            }
            return new long[]{assetNode.asInt(), amount};
        } catch (IllegalArgumentException | java.io.IOException e) {
            audit(rc, auditAction, "user:" + userId, false, "invalid input");
            sendError(rc, HttpResponseStatus.BAD_REQUEST, ERR_VALIDATION, rootMessage(e));
            return null;
        }
    }

    private ObjectNode moneyMovementResponse(long userId, int assetId, String amountStr) {
        ObjectNode resp = MAPPER.createObjectNode();
        resp.put("success", true);
        resp.put("userId", userId);
        resp.put("assetId", assetId);
        resp.put("amount", amountStr);
        return resp;
    }

    private long extractAccountUserId(String uri) {
        // /api/v1/accounts/{userId}/deposit or /withdraw
        String path = uri.substring("/api/v1/accounts/".length());
        int slash = path.indexOf('/');
        return Long.parseLong(path.substring(0, slash));
    }

    private void handleGetMarkets(RequestContext rc) throws Exception {
        var markets = orderService.getMarkets();
        sendResponse(rc, HttpResponseStatus.OK, MAPPER.writeValueAsString(markets));
    }

    // ==================== Admin Endpoints ====================

    private void handleGetRiskConfig(RequestContext rc, String uri) throws Exception {
        if (adminService == null) {
            sendError(rc, HttpResponseStatus.SERVICE_UNAVAILABLE, ERR_ADMIN_UNAVAILABLE, "Admin not available");
            return;
        }

        if (uri.equals("/api/v1/admin/risk/config")) {
            var configs = adminService.getAllRiskConfigs();
            sendResponse(rc, HttpResponseStatus.OK, MAPPER.writeValueAsString(configs));
        } else {
            String idStr = uri.substring("/api/v1/admin/risk/config/".length());
            int marketId = Integer.parseInt(idStr);
            var config = adminService.getRiskConfig(marketId);
            if (config == null) {
                sendError(rc, HttpResponseStatus.NOT_FOUND, ERR_NOT_FOUND, "Market not configured");
            } else {
                sendResponse(rc, HttpResponseStatus.OK, MAPPER.writeValueAsString(config));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void handleUpdateRiskConfig(RequestContext rc, String uri, FullHttpRequest request) throws Exception {
        if (adminService == null) {
            sendError(rc, HttpResponseStatus.SERVICE_UNAVAILABLE, ERR_ADMIN_UNAVAILABLE, "Admin not available");
            return;
        }

        String idStr = uri.substring("/api/v1/admin/risk/config/".length());
        int marketId = Integer.parseInt(idStr);
        String body = request.content().toString(StandardCharsets.UTF_8);
        Map<String, Object> fields = MAPPER.readValue(body, Map.class);

        adminService.updateRiskConfig(marketId, fields);
        audit(rc, "admin.risk.update", "market:" + marketId, true, String.valueOf(fields.keySet()));

        ObjectNode resp = MAPPER.createObjectNode();
        resp.put("success", true);
        resp.put("marketId", marketId);
        sendResponse(rc, HttpResponseStatus.OK, MAPPER.writeValueAsString(resp));
    }

    private void handleCircuitBreaker(RequestContext rc, String uri) throws Exception {
        if (adminService == null) {
            sendError(rc, HttpResponseStatus.SERVICE_UNAVAILABLE, ERR_ADMIN_UNAVAILABLE, "Admin not available");
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
            sendError(rc, HttpResponseStatus.BAD_REQUEST, ERR_VALIDATION, "Unknown action: " + action);
            return;
        }
        audit(rc, "admin.circuit-breaker", "market:" + marketId, true, action);

        ObjectNode resp = MAPPER.createObjectNode();
        resp.put("success", true);
        resp.put("marketId", marketId);
        resp.put("action", action);
        sendResponse(rc, HttpResponseStatus.OK, MAPPER.writeValueAsString(resp));
    }

    private void sendResponse(RequestContext rc, HttpResponseStatus status, String json) {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        ByteBuf content = Unpooled.wrappedBuffer(bytes);
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
        corsPolicy.apply(rc.origin(), response);
        finishResponse(rc, response);
    }

    private void sendCorsPreflightResponse(RequestContext rc) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);
        corsPolicy.apply(rc.origin(), response);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
        finishResponse(rc, response);
    }

    /**
     * Real HTTP/1.1 keep-alive (oms#66): stamp Connection per the client's
     * request ({@link HttpUtil#setKeepAlive} writes keep-alive/close correctly
     * for the protocol version) and close the socket ONLY when the caller did
     * not ask to keep it. Content-Length is already set on every path, so the
     * response is self-delimiting and the connection is safe to reuse.
     */
    private static void finishResponse(RequestContext rc, FullHttpResponse response) {
        HttpUtil.setKeepAlive(response, rc.keepAlive());
        ChannelFuture future = rc.ctx().writeAndFlush(response);
        if (!rc.keepAlive()) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private static String escapeJson(String s) {
        if (s == null) return "null";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /** The frozen error shape (oms#39): {"error": <human text>, "code": <ERR_*>}. */
    private void sendError(RequestContext rc, HttpResponseStatus status, String code, String message) {
        sendResponse(rc, status,
                "{\"error\":\"" + escapeJson(message) + "\",\"code\":\"" + code + "\"}");
    }

    /** Innermost cause message — Jackson wraps ours in layers of paths/locations. */
    private static String rootMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String m = root.getMessage();
        return m != null ? m : "malformed request body";
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // A protocol/handler fault leaves the connection in an unknown state:
        // always close, regardless of keep-alive.
        log.error("Unhandled exception in REST handler", cause);
        ctx.close();
    }
}
