package com.openexchange.oms.api.grpc;

import com.openexchange.oms.api.OrderService;
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

    // Streaming subscriptions
    private final Map<Long, Set<StreamObserver<OrderUpdate>>> orderStreamsByUser = new ConcurrentHashMap<>();
    private final Map<Long, Set<StreamObserver<ExecutionReport>>> executionStreamsByUser = new ConcurrentHashMap<>();

    public GrpcOrderService(OrderService orderService) {
        this.orderService = orderService;
    }

    @Override
    public void createOrder(com.openexchange.oms.grpc.CreateOrderRequest request,
                             StreamObserver<com.openexchange.oms.grpc.CreateOrderResponse> responseObserver) {
        try {
            CreateOrderRequest dto = new CreateOrderRequest();
            dto.setUserId(request.getUserId());
            dto.setMarketId(request.getMarketId());
            dto.setSide(request.getSide());
            dto.setOrderType(request.getOrderType());
            dto.setTimeInForce(request.getTimeInForce().isEmpty() ? null : request.getTimeInForce());
            dto.setPrice(request.getPrice());
            dto.setQuantity(request.getQuantity());
            dto.setStopPrice(request.getStopPrice());
            dto.setTrailingDelta(request.getTrailingDelta());
            dto.setDisplayQuantity(request.getDisplayQuantity());
            dto.setExpiresAtMs(request.getExpiresAtMs());
            dto.setClientOrderId(request.getClientOrderId().isEmpty() ? null : request.getClientOrderId());

            CreateOrderResponse resp = orderService.createOrder(dto);

            com.openexchange.oms.grpc.CreateOrderResponse grpcResp =
                    com.openexchange.oms.grpc.CreateOrderResponse.newBuilder()
                            .setAccepted(resp.isAccepted())
                            .setOmsOrderId(resp.getOmsOrderId())
                            .setStatus(resp.getStatus() != null ? resp.getStatus() : "")
                            .setRejectReason(resp.getRejectReason() != null ? resp.getRejectReason() : "")
                            .build();

            responseObserver.onNext(grpcResp);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("gRPC createOrder failed", e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void cancelOrder(CancelOrderRequest request,
                             StreamObserver<com.openexchange.oms.grpc.CancelOrderResponse> responseObserver) {
        try {
            CancelOrderResponse resp = orderService.cancelOrder(request.getOmsOrderId());

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
            if (resp == null) {
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
    public void streamOrders(StreamRequest request, StreamObserver<OrderUpdate> responseObserver) {
        long userId = request.getUserId();
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
        long userId = request.getUserId();
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
                .setPrice(resp.getPrice())
                .setQuantity(resp.getQuantity())
                .setFilledQty(resp.getFilledQty())
                .setRemainingQty(resp.getRemainingQty())
                .setStopPrice(resp.getStopPrice())
                .setStatus(resp.getStatus() != null ? resp.getStatus() : "")
                .setRejectReason(resp.getRejectReason() != null ? resp.getRejectReason() : "")
                .setCreatedAtMs(resp.getCreatedAtMs())
                .setUpdatedAtMs(resp.getUpdatedAtMs())
                .build();
    }
}
