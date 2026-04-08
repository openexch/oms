package com.openexchange.oms.common.domain;

import com.openexchange.oms.common.enums.OrderSide;

/**
 * Execution report representing a single fill in a trade.
 * Published to clients via WebSocket/gRPC streams.
 */
public class ExecutionReport {

    private long executionId;
    private long tradeId;
    private long omsOrderId;
    private long clusterOrderId;
    private long userId;
    private int marketId;
    private OrderSide side;
    private long price;          // Fixed-point fill price
    private long quantity;       // Fixed-point fill quantity
    private long remainingQty;   // Fixed-point remaining after fill
    private long filledQty;      // Fixed-point cumulative filled
    private boolean isMaker;
    private long executedAtMs;

    public long getExecutionId() { return executionId; }
    public void setExecutionId(long executionId) { this.executionId = executionId; }

    public long getTradeId() { return tradeId; }
    public void setTradeId(long tradeId) { this.tradeId = tradeId; }

    public long getOmsOrderId() { return omsOrderId; }
    public void setOmsOrderId(long omsOrderId) { this.omsOrderId = omsOrderId; }

    public long getClusterOrderId() { return clusterOrderId; }
    public void setClusterOrderId(long clusterOrderId) { this.clusterOrderId = clusterOrderId; }

    public long getUserId() { return userId; }
    public void setUserId(long userId) { this.userId = userId; }

    public int getMarketId() { return marketId; }
    public void setMarketId(int marketId) { this.marketId = marketId; }

    public OrderSide getSide() { return side; }
    public void setSide(OrderSide side) { this.side = side; }

    public long getPrice() { return price; }
    public void setPrice(long price) { this.price = price; }

    public long getQuantity() { return quantity; }
    public void setQuantity(long quantity) { this.quantity = quantity; }

    public long getRemainingQty() { return remainingQty; }
    public void setRemainingQty(long remainingQty) { this.remainingQty = remainingQty; }

    public long getFilledQty() { return filledQty; }
    public void setFilledQty(long filledQty) { this.filledQty = filledQty; }

    public boolean isMaker() { return isMaker; }
    public void setMaker(boolean maker) { isMaker = maker; }

    public long getExecutedAtMs() { return executedAtMs; }
    public void setExecutedAtMs(long executedAtMs) { this.executedAtMs = executedAtMs; }
}
