// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.core;

import com.openexchange.oms.common.domain.*;
import com.openexchange.oms.common.enums.*;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Central coordination engine — the "brain" of the OMS.
 * Runs on the OMS Core Thread (single-writer principle).
 * <p>
 * Consumes Disruptor events from the Aeron polling thread and orchestrates:
 * - Order lifecycle transitions
 * - Synthetic order trigger evaluation
 * - Ledger settlement calculations
 * - Redis hot state updates
 */
public class OmsCoreEngine {

    private static final Logger log = LoggerFactory.getLogger(OmsCoreEngine.class);

    private final OrderLifecycleManager lifecycleManager;
    private final SyntheticOrderEngine syntheticEngine;

    // Pluggable settlement handler (connects to LedgerService)
    private SettlementHandler settlementHandler;
    // Pluggable persistence handler
    private PersistenceHandler persistenceHandler;
    // Pluggable cluster submit handler
    private ClusterSubmitHandler clusterSubmitHandler;
    private Runnable postReconcileHook;

    // Reconcile (post-reconnect / leader-switchover): after a switchover, a cancel or its terminal
    // egress can be lost at the seam, leaving OMS holding an order it already tried to cancel (oms#21).
    // On reconnect we re-submit cancels for such orders. Deferred so the cluster's egress redelivery
    // settles first. Flag set off-thread (polling) and consumed on the GTD timer thread.
    private volatile int reconcileRoundsLeft = 0;
    private volatile long reconcileDueMs = 0;
    private static final long RECONCILE_DELAY_MS = 3_000;   // initial delay (let egress redelivery settle)
    private static final long RECONCILE_RETRY_MS = 3_000;   // between retry rounds
    private static final int RECONCILE_MAX_ROUNDS = 10;     // bound — a re-cancel lost during leader
                                                            // stabilization is retried until it lands
    // Replace-pending fallback (oms#67): an amend whose egress is entirely lost is aborted
    // after this long and handed to membership repair. Comfortably above the engine
    // round-trip, below anything a user would notice as a wedged order.
    private static final long REPLACE_PENDING_TIMEOUT_MS = 10_000;

    public OmsCoreEngine(OrderLifecycleManager lifecycleManager, SyntheticOrderEngine syntheticEngine) {
        this.lifecycleManager = lifecycleManager;
        this.syntheticEngine = syntheticEngine;

        // Wire synthetic trigger callback to create child orders
        syntheticEngine.setTriggerCallback(this::onSyntheticTriggered);
        // Refill slices go through the same public submitIcebergSlice(...) used for
        // the FIRST slice at order creation (oms#82) — one path, not two.
        syntheticEngine.setIcebergCallback(this::submitIcebergSlice);
    }

    public void setSettlementHandler(SettlementHandler handler) { this.settlementHandler = handler; }
    public void setPersistenceHandler(PersistenceHandler handler) { this.persistenceHandler = handler; }
    public void setClusterSubmitHandler(ClusterSubmitHandler handler) { this.clusterSubmitHandler = handler; }

    // ==================== Egress Event Processing ====================

    /**
     * Process OrderStatusBatch entry from cluster egress.
     * Called on OMS Core Thread via Disruptor.
     *
     * @param rejectReason engine reject reason string (match#75), already mapped from the raw SBE
     *                     code by the transport adapter; null when the egress carried no reason.
     *                     The lifecycle manager applies it only on a genuine REJECTED terminal.
     */
    public void onClusterOrderStatus(int marketId, long clusterOrderId, long userId, int status,
                                      long price, long remainingQty, long filledQty,
                                      boolean isBuy, long omsOrderId, String rejectReason) {
        OmsOrder order = lifecycleManager.onClusterOrderStatus(omsOrderId, clusterOrderId, status,
            remainingQty, filledQty, rejectReason);

        if (order == null) return;

        // Handle FOK/IOC: if order rests on book (NEW/PARTIALLY_FILLED), cancel it
        if (order.getTimeInForce() == TimeInForce.FOK) {
            if (status == 0 || status == 1) { // NEW or PARTIALLY_FILLED
                // FOK requires full fill — cancel the resting order
                requestCancel(order);
            }
        } else if (order.getTimeInForce() == TimeInForce.IOC) {
            if (status == 0) { // NEW (resting, no fills) — cancel
                requestCancel(order);
            } else if (status == 1) { // PARTIALLY_FILLED — cancel remainder
                requestCancel(order);
            }
        }

        // Persist order update
        if (persistenceHandler != null) {
            persistenceHandler.persistOrderUpdate(order);
        }
    }

    /**
     * Process TradeExecutionBatch entry from cluster egress.
     * Called on OMS Core Thread via Disruptor.
     */
    public void onTradeExecution(int marketId, long tradeId, long takerOrderId, long makerOrderId,
                                  long takerUserId, long makerUserId, long tradePrice,
                                  long tradeQuantity, boolean takerIsBuy,
                                  long takerOmsOrderId, long makerOmsOrderId) {
        // Settle the trade via ledger. settleTrade is idempotent on tradeId: the cluster re-delivers
        // egress to a client that reconnects across a leader switchover, so the same TradeExecution
        // can arrive more than once. `applied` is false for a duplicate.
        boolean applied = true;
        if (settlementHandler != null) {
            long buyerUserId = takerIsBuy ? takerUserId : makerUserId;
            long sellerUserId = takerIsBuy ? makerUserId : takerUserId;
            long buyerOmsOrderId = takerIsBuy ? takerOmsOrderId : makerOmsOrderId;
            long sellerOmsOrderId = takerIsBuy ? makerOmsOrderId : takerOmsOrderId;

            applied = settlementHandler.settleTrade(tradeId, buyerUserId, sellerUserId, marketId,
                tradePrice, tradeQuantity, buyerOmsOrderId, sellerOmsOrderId);
        }

        // Duplicate (re-delivered) trade: balances were already applied exactly once by settle().
        // Do NOT persist a second execution report or double-count filledQty.
        if (!applied) {
            return;
        }

        // Create execution reports for both taker and maker
        if (persistenceHandler != null) {
            if (takerOmsOrderId != 0) {
                ExecutionReport takerReport = createExecutionReport(tradeId, takerOmsOrderId,
                    takerOrderId, takerUserId, marketId,
                    takerIsBuy ? OrderSide.BUY : OrderSide.SELL,
                    tradePrice, tradeQuantity, false);
                persistenceHandler.persistExecution(takerReport);
            }
            if (makerOmsOrderId != 0) {
                ExecutionReport makerReport = createExecutionReport(tradeId, makerOmsOrderId,
                    makerOrderId, makerUserId, marketId,
                    takerIsBuy ? OrderSide.SELL : OrderSide.BUY,
                    tradePrice, tradeQuantity, true);
                persistenceHandler.persistExecution(makerReport);
            }
        }

        // Apply the fill to per-order filledQty from the AUTHORITATIVE TradeExecution stream.
        // (The cluster OrderStatus egress is coalesced/lossy and must not drive filledQty — it is
        // only a monotonic backstop in OrderLifecycleManager.onClusterOrderStatus.)
        OmsOrder takerOrder = takerOmsOrderId != 0
                ? lifecycleManager.applyFill(takerOmsOrderId, tradeQuantity) : null;
        OmsOrder makerOrder = makerOmsOrderId != 0
                ? lifecycleManager.applyFill(makerOmsOrderId, tradeQuantity) : null;

        if (persistenceHandler != null) {
            if (takerOrder != null) persistenceHandler.persistOrderUpdate(takerOrder);
            if (makerOrder != null) persistenceHandler.persistOrderUpdate(makerOrder);
        }

        // Iceberg slice tracking (oms#86): drain each side's per-slice counter by the trade
        // quantity and refill when the SLICE (not the parent) is exhausted. The old gate here
        // required takerOrder.getStatus()==FILLED, but applyFill accumulates against the
        // parent's TOTAL quantity, so a slice fill only ever drove the parent to
        // PARTIALLY_FILLED — the refill never fired — and it ignored the maker side entirely
        // (a RESTING slice fills as the maker). Use the orders returned by applyFill: the
        // final slice's fill makes the parent FILLED and removes it from the active map.
        trackIcebergSlice(takerOrder, takerOmsOrderId, tradeQuantity);
        trackIcebergSlice(makerOrder, makerOmsOrderId, tradeQuantity);
    }

    /** oms#86: per-slice fill tracking, side-agnostic. */
    private void trackIcebergSlice(OmsOrder order, long omsOrderId, long tradeQuantity) {
        if (order == null || order.getOrderType() != OmsOrderType.ICEBERG) {
            return;
        }
        long remaining = Math.max(0, order.getSliceRemainingQty() - tradeQuantity);
        order.setSliceRemainingQty(remaining);
        if (remaining == 0) {
            // Slice exhausted: the synthetic engine submits the next slice via
            // submitIcebergSlice (re-arming the counter), or self-cleans when the
            // hidden remainder is gone (the parent just went FILLED via applyFill).
            syntheticEngine.onIcebergSliceFilled(omsOrderId);
        }
    }

    /**
     * Process market data update from egress.
     * Updates synthetic order engine for stop/trailing evaluation.
     */
    public void onMarketDataUpdate(int marketId, long bestBid, long bestAsk) {
        syntheticEngine.onMarketDataUpdate(marketId, bestBid, bestAsk);
    }

    // ==================== Synthetic Order Handling ====================

    private void onSyntheticTriggered(OmsOrder parentOrder, OmsOrderType childType, long childPrice) {
        // The triggered synthetic order re-enters the pipeline as a child order
        // The child will go through risk → hold → cluster submission
        if (clusterSubmitHandler != null) {
            clusterSubmitHandler.submitTriggeredOrder(parentOrder, childType, childPrice);
        }
    }

    /**
     * Submit an iceberg display slice to the cluster via the pluggable submit handler.
     * This is the ONE path for every slice an iceberg ever puts on the book: the FIRST
     * slice (called directly from order creation — oms#82: an iceberg used to sit in
     * PENDING_TRIGGER forever because nothing ever called this) and every REFILL slice
     * (called here as the synthetic engine's iceberg callback, wired in the constructor).
     * Submitting both through the same method keeps egress correlation/lifecycle
     * handling identical regardless of which slice it is.
     */
    public boolean submitIcebergSlice(OmsOrder icebergOrder, long sliceQuantity) {
        // oms#86: arm the per-slice fill tracker. Slice completion is detected on the
        // TRADE stream (trackIcebergSlice), taker or maker side alike, by draining this
        // counter — the parent's cumulative status can never say "slice done".
        icebergOrder.setSliceRemainingQty(sliceQuantity);
        if (clusterSubmitHandler != null) {
            // OMS-8: propagate the enqueue result so the FIRST-slice caller (order creation)
            // can roll back a queue-full submit like the normal create path, instead of leaving
            // a PENDING_NEW iceberg with the full hold locked and nothing on the book. (The refill
            // callback wiring discards this — a queue-full refill is a separate follow-up.)
            return clusterSubmitHandler.submitIcebergSlice(icebergOrder, sliceQuantity);
        }
        return true;
    }

    // ==================== GTD Expiry ====================

    /**
     * Called by timer thread every second.
     * Checks all active GTD orders for expiry.
     */
    public void checkGtdExpiry(long nowMs) {
        // Run a pending post-reconnect reconcile on this timer thread (state mutation off the
        // polling thread, same as GTD expiry below). Retries across rounds because a re-cancel can
        // itself be lost while the just-elected leader is still stabilizing.
        if (reconcileRoundsLeft > 0 && nowMs >= reconcileDueMs) {
            int reCancelled = reconcilePendingCancels();
            reconcileRoundsLeft--;
            reconcileDueMs = nowMs + RECONCILE_RETRY_MS;
            if (reCancelled == 0) {
                reconcileRoundsLeft = 0; // converged — nothing left to reconcile
            }
        }

        // Replace timeout fallback (oms#67): lost egress must not wedge an order in
        // replace-pending forever. Abort the marker (releases the incremental hold; the
        // order keeps its original values) and request an open-orders snapshot so
        // membership repair trues up whichever leg actually survived on the cluster.
        ArrayList<OmsOrder> timedOutReplaces = new ArrayList<>();
        lifecycleManager.forEachActiveOrder(order -> {
            if (order.isReplacePending()
                    && nowMs - order.getReplaceRequestedAtMs() > REPLACE_PENDING_TIMEOUT_MS) {
                timedOutReplaces.add(order);
            }
        });
        for (OmsOrder order : timedOutReplaces) {
            lifecycleManager.abortReplace(order, "replace timed out awaiting egress");
        }
        if (!timedOutReplaces.isEmpty()) {
            requestOpenOrdersSnapshot(nowMs, "replace-pending timeout");
        }

        // Collect expired GTD orders (cannot modify map during iteration)
        ArrayList<Long> expiredIds = new ArrayList<>();
        lifecycleManager.forEachActiveOrder(order -> {
            if (order.getTimeInForce() == TimeInForce.GTD
                    && order.getExpiresAtMs() > 0
                    && nowMs >= order.getExpiresAtMs()) {
                expiredIds.add(order.getOmsOrderId());
            }
        });

        for (long omsOrderId : expiredIds) {
            OmsOrder order = lifecycleManager.onExpired(omsOrderId);
            if (order != null) {
                // Cancel the order on the cluster if it was submitted
                if (order.getClusterOrderId() != 0 && clusterSubmitHandler != null) {
                    clusterSubmitHandler.submitCancel(order.getClusterOrderId(),
                            order.getUserId(), order.getMarketId());
                }
                if (persistenceHandler != null) {
                    persistenceHandler.persistOrderUpdate(order);
                }
                log.info("GTD order expired: omsOrderId={}", omsOrderId);
            }
        }
    }

    // ==================== Reconcile ====================

    /**
     * Signal (from the cluster polling thread) that the session reconnected or the leader changed,
     * so the core thread should reconcile pending-cancel orders. Deferred by RECONCILE_DELAY_MS to
     * let the cluster's egress redelivery heal what it can first. Idempotent.
     */
    public void requestReconcile(long nowMs) {
        reconcileDueMs = nowMs + RECONCILE_DELAY_MS;
        reconcileRoundsLeft = RECONCILE_MAX_ROUNDS;
        log.info("Reconcile requested (reconnect/leader change); first round in ~{}ms, up to {} rounds",
                RECONCILE_DELAY_MS, RECONCILE_MAX_ROUNDS);
    }

    /**
     * Re-submit cancels for orders that have a cancel pending (cancelRequested) but are still active
     * in the OMS — their original cancel or its terminal egress was likely lost at a switchover seam.
     * Safe: only touches orders the user/OMS already asked to cancel (never a legitimately-resting
     * order), and the cluster now acks cancels of already-gone orders so the hold releases either way.
     */
    private int reconcilePendingCancels() {
        ArrayList<OmsOrder> toRecancel = new ArrayList<>();
        lifecycleManager.forEachActiveOrder(order -> {
            if (order.isCancelRequested()
                    && order.getClusterOrderId() != 0
                    && (order.getStatus() == OmsOrderStatus.NEW
                        || order.getStatus() == OmsOrderStatus.PARTIALLY_FILLED)) {
                toRecancel.add(order);
            }
        });
        if (toRecancel.isEmpty()) return 0;
        for (OmsOrder order : toRecancel) {
            if (clusterSubmitHandler != null) {
                clusterSubmitHandler.submitCancel(order.getClusterOrderId(), order.getUserId(),
                        order.getMarketId());
            }
        }
        log.info("Reconcile: re-submitted cancel for {} pending-cancel order(s)", toRecancel.size());
        return toRecancel.size();
    }

    /** Orders younger than this at snapshot time are never orphan-terminalized:
     *  their CreateOrder may legitimately still be in flight. */
    private static final long ORPHAN_MIN_AGE_MS = 10_000;

    /**
     * True when a submitted order has sat past ORPHAN_MIN_AGE_MS without ever
     * learning its clusterOrderId — its CreateOrder or ack was lost at a
     * switchover seam. The oms#41 failover E2E exposed the gap this closes:
     * on a QUIET cluster the post-reconnect reconcile runs while such orders
     * are still inside the age gate, no later reconcile ever triggers (no seq
     * gaps, no reconnects), and they sit PENDING_NEW forever — uncancellable
     * zombies ("Order is in-flight, please retry shortly") that eat slots.
     * The 1s timer uses this to trigger a rate-limited reconcile sweep.
     */
    public boolean hasStaleSubmittedOrphans(long nowMs) {
        final boolean[] found = {false};
        lifecycleManager.forEachActiveOrder(order -> {
            if (!found[0]
                    && order.getClusterOrderId() == 0
                    && (order.getStatus() == OmsOrderStatus.PENDING_NEW
                        || order.getStatus() == OmsOrderStatus.NEW
                        || order.getStatus() == OmsOrderStatus.PARTIALLY_FILLED)
                    && nowMs - order.getCreatedAtMs() > ORPHAN_MIN_AGE_MS) {
                found[0] = true;
            }
        });
        return found[0];
    }

    /** Delegate an OpenOrdersSnapshot request to the cluster (match#31). */
    public void requestOpenOrdersSnapshot(long requestId, String reason) {
        if (clusterSubmitHandler != null) {
            log.info("Requesting open-orders snapshot: requestId={} reason={}", requestId, reason);
            clusterSubmitHandler.submitOpenOrdersSnapshotRequest(requestId);
        }
    }

    /**
     * Membership repair (match#31 / oms#34): terminalize OMS-active orders the
     * cluster no longer has open. Their terminal statuses were lost on the wire
     * (publisher drops / batch shed / switchover seams) — the 14.05% divergence
     * measured by the P1.5 gate. Terminalizing through onClusterOrderStatus
     * reuses the normal paths, so holds are released and the per-user
     * open-order slot is freed (the oms#34 leak).
     *
     * Race guards: only orders with clusterOrderId BELOW the snapshot's orderId
     * cutoff are eligible (created-after-snapshot orders are legitimately
     * absent), and clusterOrderId-less orders are only reconciled by omsOrderId
     * when older than ORPHAN_MIN_AGE_MS at request time.
     * Fills already settled via TradeExecution decide FILLED vs CANCELLED.
     *
     * clusterOrderId-less repair covers every submitted-to-cluster state, not
     * just PENDING_NEW (oms#53): when the ack carrying the clusterOrderId is
     * lost at a seam but TradeExecutions still arrive (matched by omsOrderId),
     * the order sits NEW/PARTIALLY_FILLED with cid=0 — invisible to the
     * clusterOrderId membership check and previously outside the orphan path,
     * i.e. a permanent zombie that resurrected on every startup rebuild. Such
     * an order still open on the cluster (snapshot carries its omsOrderId) is
     * RE-LINKED by adopting the snapshot's clusterOrderId; one the cluster no
     * longer has open is terminalized like any other lost order.
     */
    public int reconcileAgainstOpenOrders(org.agrona.collections.LongHashSet clusterOpenOrderIds,
                                          org.agrona.collections.Long2LongHashMap clusterOmsToClusterId,
                                          long snapshotMaxOrderId, long requestTimeMs) {
        ArrayList<OmsOrder> toTerminalize = new ArrayList<>();
        ArrayList<OmsOrder> toRelink = new ArrayList<>();
        lifecycleManager.forEachActiveOrder(order -> {
            OmsOrderStatus st = order.getStatus();
            if (st == OmsOrderStatus.PENDING_TRIGGER) {
                return; // synthetic parent: legitimately not on the cluster book
            }
            long cid = order.getClusterOrderId();
            if (order.isReplacePending()) {
                // Cancel-and-replace in flight (oms#67): the stored cid is the old (cancelled)
                // leg — or 0 mid-swap — so the plain not-in-snapshot check would wrongly
                // terminalize a healthy amend. The new leg carries the same omsOrderId, so
                // consult the snapshot by omsOrderId: present under a different cid ⇒ the
                // new-leg egress was lost, resolve; present under the stored cid ⇒ the amend
                // has not applied yet, leave it to the replace timeout; absent ⇒ both legs
                // gone, terminalize once clearly past the leg-swap window.
                if (clusterOmsToClusterId.containsKey(order.getOmsOrderId())) {
                    if (clusterOmsToClusterId.get(order.getOmsOrderId()) != cid) {
                        toRelink.add(order);
                    }
                } else if (requestTimeMs - order.getReplaceRequestedAtMs() > ORPHAN_MIN_AGE_MS) {
                    toTerminalize.add(order);
                }
                return;
            }
            if (cid != 0) {
                if (cid < snapshotMaxOrderId && !clusterOpenOrderIds.contains(cid)) {
                    toTerminalize.add(order);
                }
            } else if ((st == OmsOrderStatus.PENDING_NEW || st == OmsOrderStatus.NEW
                    || st == OmsOrderStatus.PARTIALLY_FILLED)
                    && requestTimeMs - order.getCreatedAtMs() > ORPHAN_MIN_AGE_MS) {
                // PENDING_RISK/PENDING_HOLD stay out: pre-cluster admission states.
                if (clusterOmsToClusterId.containsKey(order.getOmsOrderId())) {
                    toRelink.add(order);
                } else {
                    toTerminalize.add(order);
                }
            }
        });
        for (OmsOrder order : toRelink) {
            long clusterOrderId = clusterOmsToClusterId.get(order.getOmsOrderId());
            if (order.isReplacePending()) {
                log.warn("Membership repair: resolving replace for omsOrderId={} to clusterOrderId={} "
                        + "(new-leg egress lost)", order.getOmsOrderId(), clusterOrderId);
                lifecycleManager.resolveReplaceFromReconcile(order.getOmsOrderId(), clusterOrderId);
            } else {
                log.warn("Membership repair: re-linking open order omsOrderId={} to clusterOrderId={} "
                        + "(ack lost, order still open on cluster)", order.getOmsOrderId(), clusterOrderId);
                lifecycleManager.onSentToCluster(order.getOmsOrderId(), clusterOrderId);
            }
            if (persistenceHandler != null) {
                persistenceHandler.persistOrderUpdate(order);
            }
        }
        for (OmsOrder order : toTerminalize) {
            // A replace-pending order reaching this point has BOTH legs missing from the
            // snapshot: release the incremental hold and clear the marker first, or the
            // pending guard in onClusterOrderStatus would swallow the repair (oms#67).
            lifecycleManager.abortReplace(order, "membership repair: both replace legs gone");
            boolean fullyFilled = order.getFilledQty() >= order.getQuantity();
            log.warn("Membership repair: terminalizing lost order omsOrderId={} clusterOrderId={} "
                            + "filled={}/{} as {}",
                    order.getOmsOrderId(), order.getClusterOrderId(),
                    order.getFilledQty(), order.getQuantity(),
                    fullyFilled ? "FILLED" : "CANCELLED");
            OmsOrder repaired = lifecycleManager.onClusterOrderStatus(
                    order.getOmsOrderId(), order.getClusterOrderId(),
                    fullyFilled ? 2 : 3, // cluster raw status: FILLED : CANCELLED
                    fullyFilled ? 0L : order.getRemainingQty(), order.getFilledQty());
            // Persist the repair: this path bypasses onOrderStatus (which persists),
            // and unpersisted repairs would resurrect on the next startup rebuild
            // (oms#35) and be re-repaired forever.
            if (repaired != null && persistenceHandler != null) {
                persistenceHandler.persistOrderUpdate(repaired);
            }
        }
        if (!toTerminalize.isEmpty()) {
            log.info("Membership repair terminalized {} lost order(s)", toTerminalize.size());
        }
        totalRepairedOrders += toTerminalize.size();
        totalRelinkedOrders += toRelink.size();
        // Post-reconcile audit hook (oms#49): lifecycle state is freshly trued
        // up against the cluster here, making this the right moment to
        // rebaseline derived bookkeeping (risk open-order slot counts) that
        // can drift when status transitions are dropped at switchover seams.
        if (postReconcileHook != null) {
            postReconcileHook.run();
        }
        return toTerminalize.size();
    }

    /** See reconcileAgainstOpenOrders: runs after every membership reconcile. */
    public void setPostReconcileHook(Runnable hook) {
        this.postReconcileHook = hook;
    }

    // Cumulative reconcile-repair tallies — read by /metrics gauges (oms#38).
    private volatile long totalRepairedOrders;
    private volatile long totalRelinkedOrders;

    public long getTotalRepairedOrders() {
        return totalRepairedOrders;
    }

    public long getTotalRelinkedOrders() {
        return totalRelinkedOrders;
    }

    // ==================== Helpers ====================

    private void requestCancel(OmsOrder order) {
        if (clusterSubmitHandler != null && order.getClusterOrderId() != 0) {
            // Mark cancel-intent so the reconcile can re-cancel if this is lost at a switchover seam.
            order.setCancelRequested(true);
            clusterSubmitHandler.submitCancel(order.getClusterOrderId(), order.getUserId(),
                order.getMarketId());
        }
    }

    private ExecutionReport createExecutionReport(long tradeId, long omsOrderId, long clusterOrderId,
                                                   long userId, int marketId, OrderSide side,
                                                   long price, long quantity, boolean isMaker) {
        ExecutionReport report = new ExecutionReport();
        report.setTradeId(tradeId);
        report.setOmsOrderId(omsOrderId);
        report.setClusterOrderId(clusterOrderId);
        report.setUserId(userId);
        report.setMarketId(marketId);
        report.setSide(side);
        report.setPrice(price);
        report.setQuantity(quantity);
        report.setMaker(isMaker);
        report.setExecutedAtMs(System.currentTimeMillis());
        return report;
    }

    public OrderLifecycleManager getLifecycleManager() { return lifecycleManager; }
    public SyntheticOrderEngine getSyntheticEngine() { return syntheticEngine; }

    // ==================== Handler Interfaces ====================

    public interface SettlementHandler {
        /**
         * Settle a trade. Idempotent on tradeId (the cluster re-delivers egress on leader
         * switchover, so the same trade can arrive more than once).
         *
         * @return true if the trade was newly applied; false if it was a duplicate and skipped.
         */
        boolean settleTrade(long tradeId, long buyerUserId, long sellerUserId, int marketId,
                        long price, long quantity, long buyerOmsOrderId, long sellerOmsOrderId);
    }

    public interface PersistenceHandler {
        void persistOrderUpdate(OmsOrder order);
        void persistExecution(ExecutionReport report);
    }

    public interface ClusterSubmitHandler {
        void submitTriggeredOrder(OmsOrder parentOrder, OmsOrderType childType, long childPrice);
        /** @return false when the cluster ingress queue was full (the slice was NOT enqueued). */
        boolean submitIcebergSlice(OmsOrder icebergOrder, long sliceQuantity);
        void submitCancel(long clusterOrderId, long userId, int marketId);
        /** match#31: ask the cluster for an OpenOrdersSnapshot egress. */
        void submitOpenOrdersSnapshotRequest(long requestId);
    }
}
