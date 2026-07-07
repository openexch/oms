// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.common.domain;

import com.openexchange.oms.common.enums.*;

/**
 * Full OMS order entity extending the match engine's basic Order.
 * Uses fixed-point long values (8 decimal places) for all prices/quantities.
 */
public class OmsOrder {

    // === Identifiers ===
    private long omsOrderId;          // Snowflake ID assigned by OMS
    private long clusterOrderId;      // Assigned by matching engine
    private String clientOrderId;     // User-supplied idempotency key

    // === Core Fields ===
    private long userId;
    private int marketId;
    private OrderSide side;
    private OmsOrderType orderType;
    private long price;               // Fixed-point limit price
    private long quantity;            // Fixed-point original quantity
    private long filledQty;           // Fixed-point total filled
    private long remainingQty;        // Fixed-point remaining

    // === Synthetic Order Fields ===
    private long stopPrice;           // Fixed-point stop trigger price
    private long trailingDelta;       // Fixed-point trailing amount
    private long trailingArmPrice;    // Fixed-point price extreme since placement
    private long displayQuantity;     // Fixed-point visible slice (iceberg)
    private long hiddenQuantity;      // Fixed-point hidden remaining (iceberg)

    // === Time-in-Force ===
    private TimeInForce timeInForce;
    private long expiresAtMs;         // Epoch millis for GTD orders

    // === Status ===
    private OmsOrderStatus status;
    private String rejectReason;

    // === Reconcile ===
    // True once a cancel has been submitted to the cluster for this order. The post-reconnect
    // reconcile re-submits cancels for flagged-but-still-active orders whose cancel or terminal
    // egress was lost at a leader-switchover seam (oms#21) — without touching legitimately-resting orders.
    private boolean cancelRequested;

    // === Ledger ===
    private long holdId;              // Reference to ledger hold
    private long holdAmount;          // Total amount held

    // === Cancel-and-replace (oms#67) ===
    // While a PUT amend is in flight, one omsOrderId spans two cluster orders: the engine
    // cancels the old leg and places a new one carrying the same omsOrderId. These fields
    // mark that window so the old leg's CANCELLED does not terminalize the order and the
    // new leg's first status re-links + applies the amended values. Volatile: set on a
    // Netty thread (updateOrder), read/cleared on the core egress thread.
    private volatile long replacePendingOldClusterOrderId; // != 0 ⇒ replace in flight
    private volatile long pendingPrice;                    // amended limit price (parent-total semantics)
    private volatile long pendingQuantity;                 // amended TOTAL quantity (incl. prior fills)
    private volatile long pendingHoldDelta;                // incremental hold placed at submit (rollback amount)
    private volatile long pendingHoldTarget;               // holdAmount to install at resolution
    private volatile long replaceRequestedAtMs;            // timeout fallback anchor

    // === Timestamps ===
    private long createdAtMs;
    private long updatedAtMs;

    // === Child Order (for synthetic) ===
    private long parentOmsOrderId;    // 0 if not a child order

    public OmsOrder() {
    }

    // === Getters and Setters ===

    public long getOmsOrderId() { return omsOrderId; }
    public void setOmsOrderId(long omsOrderId) { this.omsOrderId = omsOrderId; }

    public long getClusterOrderId() { return clusterOrderId; }
    public void setClusterOrderId(long clusterOrderId) { this.clusterOrderId = clusterOrderId; }

    public String getClientOrderId() { return clientOrderId; }
    public void setClientOrderId(String clientOrderId) { this.clientOrderId = clientOrderId; }

    public long getUserId() { return userId; }
    public void setUserId(long userId) { this.userId = userId; }

    public int getMarketId() { return marketId; }
    public void setMarketId(int marketId) { this.marketId = marketId; }

    public OrderSide getSide() { return side; }
    public void setSide(OrderSide side) { this.side = side; }

    public OmsOrderType getOrderType() { return orderType; }
    public void setOrderType(OmsOrderType orderType) { this.orderType = orderType; }

    public long getPrice() { return price; }
    public void setPrice(long price) { this.price = price; }

    public long getQuantity() { return quantity; }
    public void setQuantity(long quantity) { this.quantity = quantity; }

    public long getFilledQty() { return filledQty; }
    public void setFilledQty(long filledQty) { this.filledQty = filledQty; }

    public long getRemainingQty() { return remainingQty; }
    public void setRemainingQty(long remainingQty) { this.remainingQty = remainingQty; }

    public long getStopPrice() { return stopPrice; }
    public void setStopPrice(long stopPrice) { this.stopPrice = stopPrice; }

    public long getTrailingDelta() { return trailingDelta; }
    public void setTrailingDelta(long trailingDelta) { this.trailingDelta = trailingDelta; }

    public long getTrailingArmPrice() { return trailingArmPrice; }
    public void setTrailingArmPrice(long trailingArmPrice) { this.trailingArmPrice = trailingArmPrice; }

    public long getDisplayQuantity() { return displayQuantity; }
    public void setDisplayQuantity(long displayQuantity) { this.displayQuantity = displayQuantity; }

    public long getHiddenQuantity() { return hiddenQuantity; }
    public void setHiddenQuantity(long hiddenQuantity) { this.hiddenQuantity = hiddenQuantity; }

    public TimeInForce getTimeInForce() { return timeInForce; }
    public void setTimeInForce(TimeInForce timeInForce) { this.timeInForce = timeInForce; }

    public long getExpiresAtMs() { return expiresAtMs; }
    public void setExpiresAtMs(long expiresAtMs) { this.expiresAtMs = expiresAtMs; }

    public OmsOrderStatus getStatus() { return status; }
    public void setStatus(OmsOrderStatus status) { this.status = status; }

    public String getRejectReason() { return rejectReason; }
    public void setRejectReason(String rejectReason) { this.rejectReason = rejectReason; }

    public long getHoldId() { return holdId; }
    public void setHoldId(long holdId) { this.holdId = holdId; }

    public long getHoldAmount() { return holdAmount; }
    public void setHoldAmount(long holdAmount) { this.holdAmount = holdAmount; }

    public boolean isReplacePending() { return replacePendingOldClusterOrderId != 0; }
    public long getReplacePendingOldClusterOrderId() { return replacePendingOldClusterOrderId; }
    public void setReplacePendingOldClusterOrderId(long v) { this.replacePendingOldClusterOrderId = v; }
    public long getPendingPrice() { return pendingPrice; }
    public void setPendingPrice(long v) { this.pendingPrice = v; }
    public long getPendingQuantity() { return pendingQuantity; }
    public void setPendingQuantity(long v) { this.pendingQuantity = v; }
    public long getPendingHoldDelta() { return pendingHoldDelta; }
    public void setPendingHoldDelta(long v) { this.pendingHoldDelta = v; }
    public long getPendingHoldTarget() { return pendingHoldTarget; }
    public void setPendingHoldTarget(long v) { this.pendingHoldTarget = v; }
    public long getReplaceRequestedAtMs() { return replaceRequestedAtMs; }
    public void setReplaceRequestedAtMs(long v) { this.replaceRequestedAtMs = v; }

    public long getCreatedAtMs() { return createdAtMs; }
    public void setCreatedAtMs(long createdAtMs) { this.createdAtMs = createdAtMs; }

    public long getUpdatedAtMs() { return updatedAtMs; }
    public void setUpdatedAtMs(long updatedAtMs) { this.updatedAtMs = updatedAtMs; }

    public long getParentOmsOrderId() { return parentOmsOrderId; }
    public void setParentOmsOrderId(long parentOmsOrderId) { this.parentOmsOrderId = parentOmsOrderId; }

    public boolean isCancelRequested() { return cancelRequested; }
    public void setCancelRequested(boolean cancelRequested) { this.cancelRequested = cancelRequested; }

    public boolean isBuy() { return side == OrderSide.BUY; }
    public boolean isTerminal() { return status != null && status.isTerminal(); }
}
