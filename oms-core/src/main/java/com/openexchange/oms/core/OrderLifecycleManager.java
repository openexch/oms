// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.core;

import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.common.enums.OmsOrderType;
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
     * Ledger-side effects of a cancel-and-replace (oms#67), wired by the application so the
     * lifecycle stays ledger-agnostic. Called on the thread processing the resolving event.
     */
    public interface ReplaceHooks {
        /**
         * The replace resolved (new leg linked): apply the negative hold delta if any and
         * install the pending hold target as the order's holdAmount. The pending* fields are
         * still populated when this runs.
         */
        void onReplaceResolved(OmsOrder order);

        /**
         * The replace aborted (amend rejected, timed out, or the order went terminal first):
         * release the incremental hold placed at submit ({@code getPendingHoldDelta()}), if any.
         * The order's price/quantity/holdAmount are unchanged.
         */
        void onReplaceAborted(OmsOrder order);
    }

    private ReplaceHooks replaceHooks;

    public void setReplaceHooks(ReplaceHooks replaceHooks) {
        this.replaceHooks = replaceHooks;
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
     * Transition: PENDING_NEW → REJECTED (cluster submission enqueue failed, oms#85).
     * <p>
     * createOrder advances the order to PENDING_NEW the moment the ledger hold lands
     * ({@link #onHoldPlaced}), BEFORE it enqueues the submission to the cluster client. When
     * that bounded MPSC enqueue fails (queue momentarily full), the order is a PENDING_NEW the
     * cluster has never seen. The old queue-full path called {@link #onHoldFailed}, whose guard
     * requires PENDING_HOLD, so it no-op'd and the order lingered as an active PENDING_NEW
     * zombie (client told "rejected", yet GET still lists it and cancel refuses) until the
     * orphan sweep terminalized it ~10-20s later. Terminalize it here instead, so OMS state
     * matches the client's rejection immediately.
     * <p>
     * Guarded HARD on {@code status == PENDING_NEW && clusterOrderId == 0}: this must ONLY ever
     * terminalize an order the cluster has never acked. The hold release and the open-order slot
     * close happen in the state listener on the PENDING_NEW→REJECTED transition, so callers must
     * NOT release the hold themselves — doing both double-releases the hold (oms#85 trap).
     */
    public void onSubmitFailed(long omsOrderId, String reason) {
        OmsOrder order = activeOrders.get(omsOrderId);
        if (order == null
                || order.getStatus() != OmsOrderStatus.PENDING_NEW
                || order.getClusterOrderId() != 0) {
            return;
        }
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
     * Mark a cancel-and-replace as in flight (oms#67). Called by updateOrder BEFORE the
     * update command is enqueued, so no egress for the replace can precede the marker.
     * <p>
     * {@code pendingQuantity} carries the amended TOTAL quantity (prior fills included);
     * the leg submitted to the engine carries {@code pendingQuantity - filledQty}.
     *
     * @return false when the order is unknown, terminal, not yet on the cluster, or a
     *         replace is already pending (one amend at a time).
     */
    public boolean onReplaceSubmitted(long omsOrderId, long newPrice, long newQuantity,
                                      long holdDelta, long holdTarget) {
        OmsOrder order = activeOrders.get(omsOrderId);
        if (order == null) {
            return false;
        }
        // OMS-1: the guard read (isReplacePending) and the marker write must be one atomic,
        // single-winner transition per order. updateOrder runs on Netty I/O threads, so two
        // concurrent amends on the SAME order could otherwise both observe no pending replace,
        // both return true, and each place an incremental hold — permanently double-locking
        // funds. Synchronizing the claim on the order object serializes competing amends so
        // exactly one wins. The marker is still written LAST, so a lock-free egress reader that
        // sees isReplacePending()==true also sees the pending fields (volatile-publish invariant).
        synchronized (order) {
            if (order.getStatus().isTerminal()
                    || order.getClusterOrderId() == 0 || order.isReplacePending()) {
                return false;
            }
            order.setPendingPrice(newPrice);
            order.setPendingQuantity(newQuantity);
            order.setPendingHoldDelta(holdDelta);
            order.setPendingHoldTarget(holdTarget);
            order.setReplaceRequestedAtMs(System.currentTimeMillis());
            // Set last: isReplacePending() keys off this field, so the others are visible first.
            order.setReplacePendingOldClusterOrderId(order.getClusterOrderId());
        }
        return true;
    }

    /**
     * Abort an in-flight replace (oms#67): amend rejected by the engine, submit failed,
     * timed out, or the order went terminal first. Releases the incremental hold via the
     * hooks and clears the marker; the order keeps its original price/quantity/holds.
     */
    public void abortReplace(OmsOrder order, String why) {
        if (!order.isReplacePending()) {
            return;
        }
        log.info("Replace aborted for omsOrderId={}: {}", order.getOmsOrderId(), why);
        if (replaceHooks != null) {
            replaceHooks.onReplaceAborted(order);
        }
        clearReplacePending(order);
    }

    /**
     * Resolve an in-flight replace onto its new cluster leg (oms#67): re-index
     * byClusterOrderId, apply the ledger resolution via the hooks, install the amended
     * price/quantity, and clear the marker. Idempotent per resolution (marker cleared).
     */
    private void resolveReplace(OmsOrder order, long newClusterOrderId) {
        long oldCid = order.getClusterOrderId();
        if (oldCid != 0) {
            byClusterOrderId.remove(oldCid, order);
        }
        order.setClusterOrderId(newClusterOrderId);
        byClusterOrderId.put(newClusterOrderId, order);
        if (replaceHooks != null) {
            replaceHooks.onReplaceResolved(order);
        }
        order.setPrice(order.getPendingPrice());
        order.setQuantity(order.getPendingQuantity());
        order.setRemainingQty(Math.max(0, order.getQuantity() - order.getFilledQty()));
        log.info("Replace resolved for omsOrderId={}: clusterOrderId {} -> {}, price={}, quantity={}",
                order.getOmsOrderId(), order.getReplacePendingOldClusterOrderId(),
                newClusterOrderId, order.getPrice(), order.getQuantity());
        clearReplacePending(order);
    }

    /**
     * Membership-repair entry point (oms#67): the open-orders snapshot proved the replace's
     * new leg exists under {@code newClusterOrderId} but its NEW egress was lost.
     */
    public void resolveReplaceFromReconcile(long omsOrderId, long newClusterOrderId) {
        OmsOrder order = activeOrders.get(omsOrderId);
        if (order == null || !order.isReplacePending()) {
            return;
        }
        resolveReplace(order, newClusterOrderId);
    }

    private void clearReplacePending(OmsOrder order) {
        order.setReplacePendingOldClusterOrderId(0);
        order.setPendingPrice(0);
        order.setPendingQuantity(0);
        order.setPendingHoldDelta(0);
        order.setPendingHoldTarget(0);
        order.setReplaceRequestedAtMs(0);
    }

    /**
     * Process OrderStatusUpdate from cluster egress, with no engine-supplied reject reason.
     * Used by internal terminalizations (synthetic cancels, membership-repair) that carry no
     * engine reason. Correlates via omsOrderId (primary) or clusterOrderId (fallback).
     */
    public OmsOrder onClusterOrderStatus(long omsOrderId, long clusterOrderId, int status,
                                          long remainingQty, long filledQty) {
        return onClusterOrderStatus(omsOrderId, clusterOrderId, status, remainingQty, filledQty, null);
    }

    /**
     * Process OrderStatusUpdate from cluster egress.
     * Correlates via omsOrderId (primary) or clusterOrderId (fallback).
     *
     * @param rejectReason engine reject reason (match#75), applied to the order ONLY on a genuine
     *                     REJECTED terminal (case 4). Null when the egress carried no reason. It is
     *                     intentionally NOT applied on the replace old-leg reject path (which leaves
     *                     the order live under its original terms) so a live order never carries a
     *                     misleading reject reason.
     */
    public OmsOrder onClusterOrderStatus(long omsOrderId, long clusterOrderId, int status,
                                          long remainingQty, long filledQty, String rejectReason) {
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

        // Cancel-and-replace window (oms#67). The engine emits, for one amend:
        //   CANCELLED(oldCid) + first-status(newCid)          — amend proceeded
        //   REJECTED(oldCid)                                  — amend refused, old leg intact
        //   CANCELLED(oldCid) + REJECTED/CANCELLED(newCid)    — cancelled but the new leg couldn't rest
        // The two events arrive in either order (egress is unsequenced, match#19), so route by
        // WHICH LEG the event names rather than by arrival order.
        if (order.isReplacePending() && clusterOrderId != 0) {
            long oldLegCid = order.getReplacePendingOldClusterOrderId();
            if (clusterOrderId == oldLegCid) {
                if (status == 3) {
                    // Old leg cancelled by the replace: bookkeeping only. No transition, no
                    // removal, holds untouched — the same omsOrderId lives on in the new leg.
                    byClusterOrderId.remove(oldLegCid, order);
                    if (order.getClusterOrderId() == oldLegCid) {
                        order.setClusterOrderId(0);
                    }
                    log.debug("Replace old leg cancelled for omsOrderId={} (clusterOrderId={})",
                            order.getOmsOrderId(), oldLegCid);
                    return order;
                }
                if (status == 4) {
                    // Engine refused the amend (bad price, cancel-miss): old leg intact, order
                    // stays live with its original values. Returns BEFORE the switch, so the
                    // rejectReason (match#75) is deliberately NOT applied here: a live order must
                    // never carry a reject reason.
                    abortReplace(order, "engine rejected the amend");
                    return order;
                }
                // NEW/PARTIALLY_FILLED/FILLED for the OLD leg while pending (e.g. a fill racing
                // the cancel): abort the replace bookkeeping BEFORE normal processing so a
                // terminal outcome cannot leak the incremental hold.
                if (status == 2) {
                    abortReplace(order, "old leg filled before the replace applied");
                }
            } else {
                // Any status for a different cid while pending is the NEW leg: resolve first,
                // then let normal processing handle the status itself (NEW rests; FILLED /
                // CANCELLED / REJECTED are real new-leg outcomes, incl. could-not-rest).
                resolveReplace(order, clusterOrderId);
            }
        }

        // Iceberg slice adoption (oms#86): each refill slice is a fresh cluster order carrying
        // the parent's omsOrderId. Normally the prior slice's FILLED (case 2 below) zeroes the
        // cid so the adopt block re-links; when that FILLED was coalesced away on the lossy
        // status stream, a non-terminal status for the NEXT slice arrives while the cid still
        // names the OLD slice — re-index so user cancels and membership repair target the
        // slice actually resting.
        if (order.getOrderType() == OmsOrderType.ICEBERG
                && clusterOrderId != 0 && order.getClusterOrderId() != 0
                && clusterOrderId != order.getClusterOrderId()
                && (status == 0 || status == 1)) {
            byClusterOrderId.remove(order.getClusterOrderId(), order);
            order.setClusterOrderId(clusterOrderId);
            byClusterOrderId.put(clusterOrderId, order);
            log.debug("Iceberg slice re-linked for omsOrderId={}: clusterOrderId -> {}",
                    order.getOmsOrderId(), clusterOrderId);
        }

        // STALE-LEG GUARD (oms#67): a terminal status naming a cluster leg this order no longer
        // occupies (a re-delivered old-leg echo after the replace resolved, or an iceberg's
        // already-superseded slice) must not terminalize the live order. Non-terminal stale
        // echoes fall through harmlessly (monotonic guards). An iceberg slice's own FILLED
        // names the CURRENT cid, so it is not swallowed here.
        if (clusterOrderId != 0 && order.getClusterOrderId() != 0
                && clusterOrderId != order.getClusterOrderId()
                && (status == 2 || status == 3 || status == 4)) {
            log.debug("Ignoring stale-leg terminal status for omsOrderId={}: event cid={} current cid={} status={}",
                    order.getOmsOrderId(), clusterOrderId, order.getClusterOrderId(), status);
            return order;
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
                // Iceberg SLICE FILLED (oms#86): the cluster order that filled is one display
                // slice, not the parent — the parent still has hidden quantity working. This
                // status used to setFilledQty(total) and terminalize the whole iceberg after
                // its FIRST slice (wrong filledQty, hold leaked, refills dead). Slice-level
                // bookkeeping only: unindex the finished slice and clear the cid so the next
                // slice's NEW is adopted. Refill timing is driven by the lossless trade stream
                // (trackIcebergSlice); parent-terminal comes from applyFill when cumulative
                // fills reach the total.
                if (order.getOrderType() == OmsOrderType.ICEBERG
                        && order.getFilledQty() < order.getQuantity()) {
                    if (clusterOrderId != 0) {
                        byClusterOrderId.remove(clusterOrderId, order);
                        if (order.getClusterOrderId() == clusterOrderId) {
                            order.setClusterOrderId(0);
                        }
                    }
                    log.debug("Iceberg slice FILLED for omsOrderId={} (slice cid={}, parent {}/{} filled)",
                            order.getOmsOrderId(), clusterOrderId,
                            order.getFilledQty(), order.getQuantity());
                    return order;
                }
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
                // Attach the engine reason (match#75) BEFORE the transition, so the state
                // listener's WS/gRPC push and OrderResponse.fromOrder both carry it. Only on a
                // genuine reject; null (NONE / pre-v6 / no reason) leaves the field untouched.
                if (rejectReason != null) {
                    order.setRejectReason(rejectReason);
                }
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
            // OMS-2: a trade that completes the order while a replace is in flight must abort the
            // replace — releasing the incremental amend hold (pendingHoldDelta) via the hooks —
            // before the order is terminalized and removed, or that hold is leaked. Mirrors the
            // status==2 "old leg filled before the replace applied" path in onClusterOrderStatus.
            // No-op when no replace is pending.
            abortReplace(order, "order fully filled before the replace applied");
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
