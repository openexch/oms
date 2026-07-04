// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.dto;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.openexchange.oms.common.domain.OmsOrder;

/**
 * Response DTO for order queries.
 *
 * Money crosses the wire as exact 8-dp decimal strings, and 64-bit Snowflake
 * ids as JSON strings (oms#39): JSON numbers round both through IEEE doubles
 * (JS Number.MAX_SAFE_INTEGER is 2^53-1; trading-ui#25 saw real ids rounded).
 */
public class OrderResponse {

    @JsonSerialize(using = ToStringSerializer.class)
    private long omsOrderId;
    @JsonSerialize(using = ToStringSerializer.class)
    private long clusterOrderId;
    private String clientOrderId;
    private long userId;
    private int marketId;
    private String side;
    private String orderType;
    private String timeInForce;
    @JsonSerialize(using = FixedPointJson.Serializer.class)
    private long price;
    @JsonSerialize(using = FixedPointJson.Serializer.class)
    private long quantity;
    @JsonSerialize(using = FixedPointJson.Serializer.class)
    private long filledQty;
    @JsonSerialize(using = FixedPointJson.Serializer.class)
    private long remainingQty;
    @JsonSerialize(using = FixedPointJson.Serializer.class)
    private long stopPrice;
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
        r.price = order.getPrice();
        r.quantity = order.getQuantity();
        r.filledQty = order.getFilledQty();
        r.remainingQty = order.getRemainingQty();
        r.stopPrice = order.getStopPrice();
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
    public long getPrice() { return price; }
    public long getQuantity() { return quantity; }
    public long getFilledQty() { return filledQty; }
    public long getRemainingQty() { return remainingQty; }
    public long getStopPrice() { return stopPrice; }
    public String getStatus() { return status; }
    public String getRejectReason() { return rejectReason; }
    public long getCreatedAtMs() { return createdAtMs; }
    public long getUpdatedAtMs() { return updatedAtMs; }
}
