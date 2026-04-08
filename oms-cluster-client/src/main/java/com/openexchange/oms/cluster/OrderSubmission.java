package com.openexchange.oms.cluster;

import com.match.infrastructure.generated.OrderSide;
import com.match.infrastructure.generated.OrderType;

/**
 * Immutable order submission queued via MPSC for thread-safe delivery
 * from API threads to the single-threaded Aeron polling loop.
 */
public final class OrderSubmission {

    public enum Type {
        CREATE,
        CANCEL
    }

    private final Type type;

    // Create order fields
    private final long userId;
    private final int marketId;
    private final long price;
    private final long quantity;
    private final long totalPrice;
    private final OrderType orderType;
    private final OrderSide orderSide;
    private final long omsOrderId;

    // Cancel order fields
    private final long orderId;

    private OrderSubmission(
            Type type,
            long userId,
            int marketId,
            long price,
            long quantity,
            long totalPrice,
            OrderType orderType,
            OrderSide orderSide,
            long omsOrderId,
            long orderId) {
        this.type = type;
        this.userId = userId;
        this.marketId = marketId;
        this.price = price;
        this.quantity = quantity;
        this.totalPrice = totalPrice;
        this.orderType = orderType;
        this.orderSide = orderSide;
        this.omsOrderId = omsOrderId;
        this.orderId = orderId;
    }

    /**
     * Create a new order submission for sending to the matching engine.
     */
    public static OrderSubmission createOrder(
            long userId,
            int marketId,
            long price,
            long quantity,
            long totalPrice,
            OrderType orderType,
            OrderSide orderSide,
            long omsOrderId) {
        return new OrderSubmission(
                Type.CREATE, userId, marketId, price, quantity, totalPrice,
                orderType, orderSide, omsOrderId, 0L);
    }

    /**
     * Create a cancel order submission.
     */
    public static OrderSubmission cancelOrder(long userId, long orderId, int marketId) {
        return new OrderSubmission(
                Type.CANCEL, userId, marketId, 0L, 0L, 0L,
                null, null, 0L, orderId);
    }

    public Type getType() {
        return type;
    }

    public long getUserId() {
        return userId;
    }

    public int getMarketId() {
        return marketId;
    }

    public long getPrice() {
        return price;
    }

    public long getQuantity() {
        return quantity;
    }

    public long getTotalPrice() {
        return totalPrice;
    }

    public OrderType getOrderType() {
        return orderType;
    }

    public OrderSide getOrderSide() {
        return orderSide;
    }

    public long getOmsOrderId() {
        return omsOrderId;
    }

    public long getOrderId() {
        return orderId;
    }

    @Override
    public String toString() {
        if (type == Type.CREATE) {
            return "OrderSubmission{CREATE, userId=" + userId +
                    ", marketId=" + marketId +
                    ", price=" + price +
                    ", qty=" + quantity +
                    ", omsOrderId=" + omsOrderId + "}";
        }
        return "OrderSubmission{CANCEL, userId=" + userId +
                ", orderId=" + orderId +
                ", marketId=" + marketId + "}";
    }
}
