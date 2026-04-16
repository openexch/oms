package com.openexchange.oms.core;

import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import org.agrona.collections.Long2ObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the lifecycle of all OMS orders.
 * Single-writer: only the OMS Core Thread mutates order state.
 * <p>
 * State machine transitions:
 *   PENDING_RISK → PENDING_HOLD → PENDING_NEW → NEW → PARTIALLY_FILLED → FILLED
 *   Any active state → CANCELLED / REJECTED / EXPIRED
 *   PENDING_NEW → PENDING_TRIGGER (synthetic orders)
 *   PENDING_TRIGGER → PENDING_RISK (when triggered, re-enters pipeline as child)
 */
public class OrderLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(OrderLifecycleManager.class);

    // Active orders indexed by omsOrderId (single-writer, no sync needed)
    private final Long2ObjectHashMap<OmsOrder> activeOrders = new Long2ObjectHashMap<>();

    // Index by clusterOrderId for egress correlation
    private final Long2ObjectHashMap<OmsOrder> byClusterOrderId = new Long2ObjectHashMap<>();

    // Listener for state transitions
    private OrderStateListener stateListener;

    public void setStateListener(OrderStateListener stateListener) {
        this.stateListener = stateListener;
    }

    /**
     * Register a new order entering the lifecycle.
     */
    public void registerOrder(OmsOrder order) {
        order.setStatus(OmsOrderStatus.PENDING_RISK);
        order.setCreatedAtMs(System.currentTimeMillis());
        activeOrders.put(order.getOmsOrderId(), order);
        log.debug("Order registered: omsOrderId={}, type={}, side={}",
            order.getOmsOrderId(), order.getOrderType(), order.getSide());
    }

    /**
     * Transition: PENDING_RISK → PENDING_HOLD (risk passed)
     */
    public void onRiskPassed(long omsOrderId) {
        OmsOrder order = activeOrders.get(omsOrderId);
        if (order == null || order.getStatus() != OmsOrderStatus.PENDING_RISK) return;
        transition(order, OmsOrderStatus.PENDING_HOLD);
    }

    /**
     * Transition: PENDING_RISK → REJECTED (risk failed)
     */
    public void onRiskRejected(long omsOrderId, String reason) {
        OmsOrder order = activeOrders.get(omsOrderId);
        if (order == null || order.getStatus() != OmsOrderStatus.PENDING_RISK) return;
        order.setRejectReason(reason);
        transition(order, OmsOrderStatus.REJECTED);
        removeOrder(omsOrderId);
    }

    /**
     * Transition: PENDING_HOLD → PENDING_NEW (hold placed)
     */
    public void onHoldPlaced(long omsOrderId) {
        OmsOrder order = activeOrders.get(omsOrderId);
        if (order == null || order.getStatus() != OmsOrderStatus.PENDING_HOLD) return;
        transition(order, OmsOrderStatus.PENDING_NEW);
    }

    /**
     * Transition: PENDING_HOLD → REJECTED (hold failed)
     */
    public void onHoldFailed(long omsOrderId, String reason) {
        OmsOrder order = activeOrders.get(omsOrderId);
        if (order == null || order.getStatus() != OmsOrderStatus.PENDING_HOLD) return;
        order.setRejectReason(reason);
        transition(order, OmsOrderStatus.REJECTED);
        removeOrder(omsOrderId);
    }

    /**
     * Transition: PENDING_NEW → PENDING_TRIGGER (synthetic order, not sent to cluster)
     */
    public void onPendingTrigger(long omsOrderId) {
        OmsOrder order = activeOrders.get(omsOrderId);
        if (order == null || order.getStatus() != OmsOrderStatus.PENDING_NEW) return;
        transition(order, OmsOrderStatus.PENDING_TRIGGER);
    }

    /**
     * Transition: PENDING_NEW → NEW (sent to cluster, accepted)
     * Also sets the clusterOrderId for egress correlation.
     */
    public void onSentToCluster(long omsOrderId, long clusterOrderId) {
        OmsOrder order = activeOrders.get(omsOrderId);
        if (order == null) return;
        order.setClusterOrderId(clusterOrderId);
        byClusterOrderId.put(clusterOrderId, order);
    }

    /**
     * Process OrderStatusUpdate from cluster egress.
     * Correlates via omsOrderId (primary) or clusterOrderId (fallback).
     */
    public OmsOrder onClusterOrderStatus(long omsOrderId, long clusterOrderId, int status,
                                          long remainingQty, long filledQty) {
        OmsOrder order = activeOrders.get(omsOrderId);
        if (order == null) {
            order = byClusterOrderId.get(clusterOrderId);
        }
        if (order == null) {
            log.warn("Unknown order in status update: omsOrderId={}, clusterOrderId={}", omsOrderId, clusterOrderId);
            return null;
        }

        // Store clusterOrderId on first status update from cluster
        if (clusterOrderId != 0 && order.getClusterOrderId() == 0) {
            order.setClusterOrderId(clusterOrderId);
            byClusterOrderId.put(clusterOrderId, order);
        }

        order.setRemainingQty(remainingQty);
        order.setFilledQty(filledQty);

        switch (status) {
            case 0: // NEW
                transition(order, OmsOrderStatus.NEW);
                break;
            case 1: // PARTIALLY_FILLED
                transition(order, OmsOrderStatus.PARTIALLY_FILLED);
                break;
            case 2: // FILLED
                transition(order, OmsOrderStatus.FILLED);
                removeOrder(order.getOmsOrderId());
                break;
            case 3: // CANCELLED
                transition(order, OmsOrderStatus.CANCELLED);
                removeOrder(order.getOmsOrderId());
                break;
            case 4: // REJECTED
                transition(order, OmsOrderStatus.REJECTED);
                removeOrder(order.getOmsOrderId());
                break;
        }

        return order;
    }

    /**
     * Process cancellation request.
     */
    public OmsOrder onCancelRequested(long omsOrderId) {
        OmsOrder order = activeOrders.get(omsOrderId);
        if (order == null) return null;
        if (order.getStatus().isTerminal()) return null;
        return order;
    }

    /**
     * Process GTD order expiry.
     */
    public OmsOrder onExpired(long omsOrderId) {
        OmsOrder order = activeOrders.get(omsOrderId);
        if (order == null || order.getStatus().isTerminal()) return null;
        transition(order, OmsOrderStatus.EXPIRED);
        removeOrder(omsOrderId);
        return order;
    }

    public OmsOrder getOrder(long omsOrderId) {
        return activeOrders.get(omsOrderId);
    }

    public OmsOrder getByClusterOrderId(long clusterOrderId) {
        return byClusterOrderId.get(clusterOrderId);
    }

    public int getActiveOrderCount() {
        return activeOrders.size();
    }

    /**
     * Iterate over all active orders. Used for queries.
     */
    public void forEachActiveOrder(java.util.function.Consumer<OmsOrder> consumer) {
        for (OmsOrder omsOrder : activeOrders.values()) {
            consumer.accept(omsOrder);
        }
    }

    private void transition(OmsOrder order, OmsOrderStatus newStatus) {
        OmsOrderStatus oldStatus = order.getStatus();
        order.setStatus(newStatus);
        order.setUpdatedAtMs(System.currentTimeMillis());
        log.debug("Order {} transitioned: {} → {}", order.getOmsOrderId(), oldStatus, newStatus);
        if (stateListener != null) {
            stateListener.onOrderStateChanged(order, oldStatus, newStatus);
        }
    }

    private void removeOrder(long omsOrderId) {
        OmsOrder removed = activeOrders.remove(omsOrderId);
        if (removed != null && removed.getClusterOrderId() != 0) {
            byClusterOrderId.remove(removed.getClusterOrderId());
        }
    }
}
