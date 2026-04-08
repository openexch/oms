package com.openexchange.oms.api.dto;

import com.openexchange.oms.common.domain.OmsOrder;
import com.match.domain.FixedPoint;

/**
 * Response DTO for order queries.
 */
public class OrderResponse {

    private long omsOrderId;
    private long clusterOrderId;
    private String clientOrderId;
    private long userId;
    private int marketId;
    private String side;
    private String orderType;
    private String timeInForce;
    private double price;
    private double quantity;
    private double filledQty;
    private double remainingQty;
    private double stopPrice;
    private String status;
    private String rejectReason;
    private long createdAtMs;
    private long updatedAtMs;

    public static OrderResponse fromOrder(OmsOrder order) {
        OrderResponse r = new OrderResponse();
        r.omsOrderId = order.getOmsOrderId();
        r.clusterOrderId = order.getClusterOrderId();
        r.clientOrderId = order.getClientOrderId();
        r.userId = order.getUserId();
        r.marketId = order.getMarketId();
        r.side = order.getSide() != null ? order.getSide().name() : null;
        r.orderType = order.getOrderType() != null ? order.getOrderType().name() : null;
        r.timeInForce = order.getTimeInForce() != null ? order.getTimeInForce().name() : null;
        r.price = FixedPoint.toDouble(order.getPrice());
        r.quantity = FixedPoint.toDouble(order.getQuantity());
        r.filledQty = FixedPoint.toDouble(order.getFilledQty());
        r.remainingQty = FixedPoint.toDouble(order.getRemainingQty());
        r.stopPrice = FixedPoint.toDouble(order.getStopPrice());
        r.status = order.getStatus() != null ? order.getStatus().name() : null;
        r.rejectReason = order.getRejectReason();
        r.createdAtMs = order.getCreatedAtMs();
        r.updatedAtMs = order.getUpdatedAtMs();
        return r;
    }

    // Getters
    public long getOmsOrderId() { return omsOrderId; }
    public long getClusterOrderId() { return clusterOrderId; }
    public String getClientOrderId() { return clientOrderId; }
    public long getUserId() { return userId; }
    public int getMarketId() { return marketId; }
    public String getSide() { return side; }
    public String getOrderType() { return orderType; }
    public String getTimeInForce() { return timeInForce; }
    public double getPrice() { return price; }
    public double getQuantity() { return quantity; }
    public double getFilledQty() { return filledQty; }
    public double getRemainingQty() { return remainingQty; }
    public double getStopPrice() { return stopPrice; }
    public String getStatus() { return status; }
    public String getRejectReason() { return rejectReason; }
    public long getCreatedAtMs() { return createdAtMs; }
    public long getUpdatedAtMs() { return updatedAtMs; }
}
