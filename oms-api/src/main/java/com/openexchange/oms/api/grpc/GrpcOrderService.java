package com.openexchange.oms.api.grpc;

import com.openexchange.oms.api.OrderService;
import com.openexchange.oms.api.audit.AuditLog;
import com.openexchange.oms.api.auth.Authorizer;
import com.openexchange.oms.api.auth.GrpcAuthInterceptor;
import com.openexchange.oms.api.auth.RoleBasedAuthorizer;
import com.openexchange.oms.api.dto.CreateOrderRequest;
import com.openexchange.oms.api.dto.CreateOrderResponse;
import com.openexchange.oms.api.dto.CancelOrderResponse;
import com.openexchange.oms.api.dto.OrderResponse;
import com.openexchange.oms.grpc.*;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * gRPC implementation of OrderService.
 */
public class GrpcOrderService extends OrderServiceGrpc.OrderServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(GrpcOrderService.class);

    private final OrderService orderService;
    private final Authorizer authorizer;
    private final AuditLog auditLog;

    // Streaming subscriptions
    private final Map<Long, Set<StreamObserver<OrderUpdate>>> orderStreamsByUser = new ConcurrentHashMap<>();
    private final Map<Long, Set<StreamObserver<ExecutionReport>>> executionStreamsByUser = new ConcurrentHashMap<>();

    public GrpcOrderService(OrderService orderService) {
        this(orderService, new RoleBasedAuthorizer(), AuditLog.disabled());
    }

    public GrpcOrderService(OrderService orderService, Authorizer authorizer, AuditLog auditLog) {
        this.orderService = orderService;
        this.authorizer = authorizer;
        this.auditLog = auditLog;
    }

    @Override
    public void createOrder(com.openexchange.oms.grpc.CreateOrderRequest request,
                             StreamObserver<com.openexchange.oms.grpc.CreateOrderResponse> responseObserver) {
        try {
            Long userId = GrpcAuth.resolveUserId(authorizer, request.getUserId(), responseObserver);
            if (userId == null) return;

            CreateOrderRequest dto = new CreateOrderRequest();
            dto.setUserId(userId);
            dto.setMarketId(request.getMarketId());
            dto.setSide(request.getSide());
            dto.setOrderType(request.getOrderType());
            dto.setTimeInForce(request.getTimeInForce().isEmpty() ? null : request.getTimeInForce());
            // Proto money is exact decimal strings (oms#39); "" = unset
            dto.setPrice(protoMoney(request.getPrice()));
            dto.setQuantity(protoMoney(request.getQuantity()));
            dto.setStopPrice(protoMoney(request.getStopPrice()));
            dto.setTrailingDelta(protoMoney(request.getTrailingDelta()));
            dto.setDisplayQuantity(protoMoney(request.getDisplayQuantity()));
            dto.setExpiresAtMs(request.getExpiresAtMs());
            dto.setClientOrderId(request.getClientOrderId().isEmpty() ? null : request.getClientOrderId());

            CreateOrderResponse resp = orderService.createOrder(dto);
            auditLog.record(GrpcAuthInterceptor.PRINCIPAL.get(), "order.create", "user:" + userId,
                    resp.isAccepted(),
                    resp.isAccepted() ? "omsOrderId=" + resp.getOmsOrderId() : resp.getRejectReason());

            com.openexchange.oms.grpc.CreateOrderResponse grpcResp =
                    com.openexchange.oms.grpc.CreateOrderResponse.newBuilder()
                            .setAccepted(resp.isAccepted())
                            .setOmsOrderId(resp.getOmsOrderId())
                            .setStatus(resp.getStatus() != null ? resp.getStatus() : "")
                            .setRejectReason(resp.getRejectReason() != null ? resp.getRejectReason() : "")
                            .setDuplicate(resp.isDuplicate())
                            .build();

            responseObserver.onNext(grpcResp);
            responseObserver.onCompleted();
        } catch (io.grpc.StatusRuntimeException e) {
            responseObserver.onError(e); // e.g. INVALID_ARGUMENT from protoMoney
        } catch (Exception e) {
            log.error("gRPC createOrder failed", e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void cancelOrder(CancelOrderRequest request,
                             StreamObserver<com.openexchange.oms.grpc.CancelOrderResponse> responseObserver) {
        try {
            // Ownership gate: not-found and not-yours are indistinguishable.
            OrderResponse existing = orderService.getOrder(request.getOmsOrderId());
            if (existing != null && !GrpcAuth.canActAs(authorizer, existing.getUserId())) {
                responseObserver.onError(io.grpc.Status.NOT_FOUND.withDescription("Order not found").asRuntimeException());
                return;
            }
            CancelOrderResponse resp = orderService.cancelOrder(request.getOmsOrderId());
            auditLog.record(GrpcAuthInterceptor.PRINCIPAL.get(), "order.cancel",
                    "order:" + request.getOmsOrderId(), resp.isAccepted(), resp.getMessage());

            com.openexchange.oms.grpc.CancelOrderResponse grpcResp =
                    com.openexchange.oms.grpc.CancelOrderResponse.newBuilder()
                            .setAccepted(resp.isAccepted())
                            .setOmsOrderId(resp.getOmsOrderId())
                            .setMessage(resp.getMessage() != null ? resp.getMessage() : "")
                            .build();

            responseObserver.onNext(grpcResp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getOrder(GetOrderRequest request,
                          StreamObserver<com.openexchange.oms.grpc.OrderResponse> responseObserver) {
        try {
            OrderResponse resp = orderService.getOrder(request.getOmsOrderId());
            if (resp == null || !GrpcAuth.canActAs(authorizer, resp.getUserId())) {
                responseObserver.onError(io.grpc.Status.NOT_FOUND.withDescription("Order not found").asRuntimeException());
                return;
            }

            responseObserver.onNext(toGrpcOrderResponse(resp));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getOrderHistory(OrderHistoryRequest request,
                                 StreamObserver<OrderHistoryResponse> responseObserver) {
        try {
            Long userId = GrpcAuth.resolveUserId(authorizer, request.getUserId(), responseObserver);
            if (userId == null) return;

            var orders = orderService.getOrderHistory(userId,
                    request.getStatus().isEmpty() ? null : request.getStatus(),
                    request.getLimit(), request.getOffset());

            OrderHistoryResponse.Builder builder = OrderHistoryResponse.newBuilder();
            for (OrderResponse order : orders) {
                builder.addOrders(toGrpcOrderResponse(order));
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("Unknown status: " + request.getStatus()).asRuntimeException());
        } catch (IllegalStateException e) {
            responseObserver.onError(io.grpc.Status.UNAVAILABLE
                    .withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            log.error("gRPC getOrderHistory failed", e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void getTrades(GetTradesRequest request,
                           StreamObserver<GetTradesResponse> responseObserver) {
        try {
            Long userId = GrpcAuth.resolveUserId(authorizer, request.getUserId(), responseObserver);
            if (userId == null) return;

            var executions = orderService.getExecutions(userId, request.getLimit(), request.getOffset());

            GetTradesResponse.Builder builder = GetTradesResponse.newBuilder();
            for (com.openexchange.oms.api.dto.ExecutionResponse exec : executions) {
                builder.addExecutions(ExecutionReport.newBuilder()
                        .setTradeId(exec.getTradeId())
                        .setOmsOrderId(exec.getOmsOrderId())
                        .setUserId(exec.getUserId())
                        .setMarketId(exec.getMarketId())
                        .setSide(exec.getSide() != null ? exec.getSide() : "")
                        .setPrice(com.match.domain.FixedPoint.format(exec.getPrice()))
                        .setQuantity(com.match.domain.FixedPoint.format(exec.getQuantity()))
                        .setIsMaker(exec.isMaker())
                        .setExecutedAtMs(exec.getExecutedAtMs())
                        .build());
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (IllegalStateException e) {
            responseObserver.onError(io.grpc.Status.UNAVAILABLE
                    .withDescription(e.getMessage()).asRuntimeException());
        } catch (Exception e) {
            log.error("gRPC getTrades failed", e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void streamOrders(StreamRequest request, StreamObserver<OrderUpdate> responseObserver) {
        Long allowed = GrpcAuth.resolveUserId(authorizer, request.getUserId(), responseObserver);
        if (allowed == null) return;
        long userId = allowed;
        Set<StreamObserver<OrderUpdate>> observers =
                orderStreamsByUser.computeIfAbsent(userId, k -> new CopyOnWriteArraySet<>());
        observers.add(responseObserver);

        if (responseObserver instanceof ServerCallStreamObserver<OrderUpdate> sco) {
            sco.setOnCancelHandler(() -> {
                observers.remove(responseObserver);
                if (observers.isEmpty()) orderStreamsByUser.remove(userId);
            });
        }
    }

    @Override
    public void streamExecutions(StreamRequest request, StreamObserver<ExecutionReport> responseObserver) {
        Long allowed = GrpcAuth.resolveUserId(authorizer, request.getUserId(), responseObserver);
        if (allowed == null) return;
        long userId = allowed;
        Set<StreamObserver<ExecutionReport>> observers =
                executionStreamsByUser.computeIfAbsent(userId, k -> new CopyOnWriteArraySet<>());
        observers.add(responseObserver);

        if (responseObserver instanceof ServerCallStreamObserver<ExecutionReport> sco) {
            sco.setOnCancelHandler(() -> {
                observers.remove(responseObserver);
                if (observers.isEmpty()) executionStreamsByUser.remove(userId);
            });
        }
    }

    /**
     * Push an order update to all streaming subscribers for a user.
     */
    public void pushOrderUpdate(long userId, OrderResponse resp) {
        Set<StreamObserver<OrderUpdate>> observers = orderStreamsByUser.get(userId);
        if (observers == null || observers.isEmpty()) return;

        OrderUpdate update = OrderUpdate.newBuilder()
                .setEventType("ORDER_UPDATE")
                .setOrder(toGrpcOrderResponse(resp))
                .build();

        for (StreamObserver<OrderUpdate> obs : observers) {
            try {
                obs.onNext(update);
            } catch (Exception e) {
                observers.remove(obs);
            }
        }
    }

    /** "" = unset (proto3 default); otherwise the exact decimal string. */
    private static long protoMoney(String s) {
        if (s == null || s.isEmpty()) {
            return 0L;
        }
        try {
            return com.match.domain.FixedPoint.parse(s);
        } catch (NumberFormatException | com.match.domain.FixedPoint.OverflowException e) {
            throw io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("invalid money string '" + s + "'").asRuntimeException();
        }
    }

    private static com.openexchange.oms.grpc.OrderResponse toGrpcOrderResponse(OrderResponse resp) {
        return com.openexchange.oms.grpc.OrderResponse.newBuilder()
                .setOmsOrderId(resp.getOmsOrderId())
                .setClusterOrderId(resp.getClusterOrderId())
                .setClientOrderId(resp.getClientOrderId() != null ? resp.getClientOrderId() : "")
                .setUserId(resp.getUserId())
                .setMarketId(resp.getMarketId())
                .setSide(resp.getSide() != null ? resp.getSide() : "")
                .setOrderType(resp.getOrderType() != null ? resp.getOrderType() : "")
                .setTimeInForce(resp.getTimeInForce() != null ? resp.getTimeInForce() : "")
                .setPrice(com.match.domain.FixedPoint.format(resp.getPrice()))
                .setQuantity(com.match.domain.FixedPoint.format(resp.getQuantity()))
                .setFilledQty(com.match.domain.FixedPoint.format(resp.getFilledQty()))
                .setRemainingQty(com.match.domain.FixedPoint.format(resp.getRemainingQty()))
                .setStopPrice(com.match.domain.FixedPoint.format(resp.getStopPrice()))
                .setStatus(resp.getStatus() != null ? resp.getStatus() : "")
                .setRejectReason(resp.getRejectReason() != null ? resp.getRejectReason() : "")
                .setCreatedAtMs(resp.getCreatedAtMs())
                .setUpdatedAtMs(resp.getUpdatedAtMs())
                .build();
    }
}
