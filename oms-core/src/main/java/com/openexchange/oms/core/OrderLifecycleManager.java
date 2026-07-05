// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.core;

import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the lifecycle of all OMS orders.
 * <p>
 * CONCURRENCY (oms#70): despite the original single-writer intent, this
 * class is mutated from MULTIPLE threads — {@code registerOrder} runs on
 * Netty I/O threads (createOrder), while cluster-egress transitions
 * ({@code onClusterOrderStatus}/{@code applyFill}) run on the OMS core
 * thread, with reads from Netty/gRPC threads throughout. Concurrent
 * structural modification of the previous Agrona maps corrupted the probe
 * chain into an INFINITE LOOP and wedged every HTTP worker (total, silent
 * REST outage — twice on 2026-07-05). The order indexes are therefore
 * ConcurrentHashMaps: lock-free reads, safe concurrent structure, and the
 * put/get happens-before edge means a status update observes a fully
 * initialized order. Do not swap these back to Agrona maps without first
 * funneling ALL mutation through one thread.
 * <p>
 * State machine transitions:
 *   PENDING_RISK → PENDING_HOLD → PENDING_NEW → NEW → PARTIALLY_FILLED → FILLED
 *   Any active state → CANCELLED / REJECTED / EXPIRED
 *   PENDING_NEW → PENDING_TRIGGER (synthetic orders)
 *   PENDING_TRIGGER → PENDING_RISK (when triggered, re-enters pipeline as child)
 */
public class OrderLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(OrderLifecycleManager.class);

    // Active orders indexed by omsOrderId (concurrent: see class doc)
    private final java.util.concurrent.ConcurrentHashMap<Long, OmsOrder> activeOrders =
            new java.util.concurrent.ConcurrentHashMap<>();

    // Index by clusterOrderId for egress correlation (concurrent: see class doc)
    private final java.util.concurrent.ConcurrentHashMap<Long, OmsOrder> byClusterOrderId =
            new java.util.concurrent.ConcurrentHashMap<>();

    // clientOrderId idempotency index (oms#40): "<userId>:<clientOrderId>" →
    // omsOrderId, ACTIVE orders only (entries leave with removeOrder, so a
    // clientOrderId becomes reusable once its order is terminal). Concurrent:
    // the duplicate check runs on Netty I/O threads before registration.
    private final java.util.concurrent.ConcurrentHashMap<String, Long> byClientOrderId =
            new java.util.concurrent.ConcurrentHashMap<>();

    private static String clientKey(long userId, String clientOrderId) {
        return userId + ":" + clientOrderId;
    }

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
        indexClientOrderId(order);
        log.debug("Order registered: omsOrderId={}, type={}, side={}",
            order.getOmsOrderId(), order.getOrderType(), order.getSide());
    }

    /**
     * Restore an order during startup state rebuild (oms#35): index it AS-IS,
     * preserving status/filledQty/createdAt from Postgres. No state transition,
     * no listener, no persistence — the order already lived through those.
     */
    public void restoreOrder(OmsOrder order) {
        activeOrders.put(order.getOmsOrderId(), order);
        if (order.getClusterOrderId() != 0) {
            byClusterOrderId.put(order.getClusterOrderId(), order);
        }
        indexClientOrderId(order);
    }

    /**
     * The omsOrderId of the caller's ACTIVE order carrying this clientOrderId,
     * or 0 when none — the idempotency lookup (oms#40).
     */
    public long findActiveByClientOrderId(long userId, String clientOrderId) {
        Long omsOrderId = byClientOrderId.get(clientKey(userId, clientOrderId));
        return omsOrderId != null ? omsOrderId : 0;
    }

    private void indexClientOrderId(OmsOrder order) {
        if (order.getClientOrderId() == null || order.getClientOrderId().isEmpty()) {
            return;
        }
        Long prior = byClientOrderId.putIfAbsent(
                clientKey(order.getUserId(), order.getClientOrderId()), order.getOmsOrderId());
        if (prior != null && prior != order.getOmsOrderId()) {
            // Lost the race with a concurrent submit that passed the duplicate
            // check in the same window; keep the first claim (best-effort dedupe).
            log.warn("clientOrderId collision: userId={} clientOrderId={} kept omsOrderId={} dropped {}",
                    order.getUserId(), order.getClientOrderId(), prior, order.getOmsOrderId());
        }
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
            // Common and benign after a leader switchover: the cluster re-delivers egress for orders
            // already terminal/removed here. Debug (not warn) to avoid log noise.
            log.debug("Unknown order in status update: omsOrderId={}, clusterOrderId={}", omsOrderId, clusterOrderId);
            return null;
        }

        // Store clusterOrderId on first status update from cluster
        if (clusterOrderId != 0 && order.getClusterOrderId() == 0) {
            order.setClusterOrderId(clusterOrderId);
            byClusterOrderId.put(clusterOrderId, order);
        }

        // MONOTONIC GUARD: the OrderStatus egress stream is coalesced/lossy and unsequenced, so a
        // stale/out-of-order update can carry a LOWER filledQty than reality. filledQty is driven
        // authoritatively by applyFill() from the lossless TradeExecution stream; here we only ever
        // RAISE it, never let the status stream regress the trade-derived value (the bug #9 fix).
        long guardedFilled = Math.max(order.getFilledQty(), filledQty);
        order.setFilledQty(guardedFilled);
        order.setRemainingQty(Math.max(0, order.getQuantity() - guardedFilled));

        switch (status) {
            case 0: // NEW — do not regress an order already advanced by trade-driven fills
                if (order.getStatus() != OmsOrderStatus.PARTIALLY_FILLED
                        && order.getStatus() != OmsOrderStatus.FILLED) {
                    transition(order, OmsOrderStatus.NEW);
                }
                break;
            case 1: // PARTIALLY_FILLED
                if (order.getStatus() != OmsOrderStatus.FILLED) {
                    transition(order, OmsOrderStatus.PARTIALLY_FILLED);
                }
                break;
            case 2: // FILLED — cluster confirms the order left the book fully filled; reconcile to qty
                order.setFilledQty(order.getQuantity());
                order.setRemainingQty(0);
                transition(order, OmsOrderStatus.FILLED);
                removeOrder(order.getOmsOrderId());
                break;
            case 3: // CANCELLED — filledQty preserved by the monotonic guard above (do not reset)
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
     * Apply a trade-derived fill to an order. This is the AUTHORITATIVE source of {@code filledQty},
     * driven by the lossless TradeExecution egress stream (the cluster OrderStatus stream is
     * coalesced/lossy and only used as a monotonic backstop in {@link #onClusterOrderStatus}).
     * <p>
     * Callers must gate this on the per-tradeId settle() dedup so re-delivered trades (which the
     * cluster replays on a leader switchover) are not double-counted here.
     *
     * @param omsOrderId the order to apply the fill to
     * @param fillQty    fixed-point fill quantity from this trade
     * @return the affected order (FILLED orders are returned even though they are removed from the
     *         active map), or null if the order is unknown or already terminal (safe no-op)
     */
    public OmsOrder applyFill(long omsOrderId, long fillQty) {
        OmsOrder order = activeOrders.get(omsOrderId);
        if (order == null || order.getStatus().isTerminal()) {
            return null;
        }
        long newFilled = order.getFilledQty() + fillQty;
        order.setFilledQty(newFilled);
        order.setRemainingQty(Math.max(0, order.getQuantity() - newFilled));
        if (newFilled >= order.getQuantity()) {
            transition(order, OmsOrderStatus.FILLED);
            removeOrder(omsOrderId);
        } else {
            transition(order, OmsOrderStatus.PARTIALLY_FILLED);
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
        // Mark cancel-intent so the post-reconnect reconcile can re-submit this cancel if it (or its
        // terminal egress) is lost at a leader-switchover seam (oms#21).
        order.setCancelRequested(true);
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
        if (removed != null && removed.getClientOrderId() != null && !removed.getClientOrderId().isEmpty()) {
            // Value-guarded: never evict a mapping this order lost to a
            // collision (see indexClientOrderId).
            byClientOrderId.remove(
                    clientKey(removed.getUserId(), removed.getClientOrderId()), removed.getOmsOrderId());
        }
    }
}
