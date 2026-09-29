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
    private volatile boolean durableStateHealthy = true;
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
    // Mark an unacknowledged replace unresolved after this interval. A timeout
    // does not prove an abort and cannot authorize rollback of its hold.
    private static final long REPLACE_PENDING_TIMEOUT_MS = 10_000;

    public OmsCoreEngine(OrderLifecycleManager lifecycleManager, SyntheticOrderEngine syntheticEngine) {
        this.lifecycleManager = lifecycleManager;
        this.syntheticEngine = syntheticEngine;

        // Wire synthetic trigger callback to create child orders
        syntheticEngine.setTriggerCallback(this::onSyntheticTriggered);
        syntheticEngine.setCheckpointCallback(this::persistOrderState);
        // Refill slices go through the same public submitIcebergSlice(...) used for
        // the FIRST slice at order creation (oms#82) — one path, not two.
        syntheticEngine.setIcebergCallback(this::submitIcebergSlice);
    }

    private volatile boolean journalAuthoritative;

    /** Orders touched by one journal trade, and icebergs whose display slice ran out. */
    public record JournalTradeResult(java.util.List<OmsOrder> touched, java.util.List<Long> exhaustedIcebergSlices) {}

    /** Cutover mode: fills and terminals come only from the verified ME journal. */
    public void setJournalAuthoritative(boolean journalAuthoritative) {
        this.journalAuthoritative = journalAuthoritative;
        lifecycleManager.setJournalAuthoritative(journalAuthoritative);
    }

    public boolean isJournalAuthoritative() { return journalAuthoritative; }

    /**
     * Apply one journal trade to both legs. Nothing is persisted and nothing is sent: the caller
     * commits the touched orders with its checkpoint, then refills the exhausted iceberg slices.
     * The caller holds the orders' monitors and guarantees each trade is applied once.
     */
    public JournalTradeResult applyJournalTrade(long takerOrderId, long makerOrderId, long takerOmsOrderId,
                                                long makerOmsOrderId, long quantity) {
        java.util.List<OmsOrder> touched = new java.util.ArrayList<>(2);
        java.util.List<Long> exhausted = new java.util.ArrayList<>(0);
        applyJournalLeg(takerOmsOrderId, takerOrderId, quantity, touched, exhausted);
        applyJournalLeg(makerOmsOrderId, makerOrderId, quantity, touched, exhausted);
        return new JournalTradeResult(touched, exhausted);
    }

    private void applyJournalLeg(long omsOrderId, long clusterLegId, long quantity,
                                 java.util.List<OmsOrder> touched, java.util.List<Long> exhausted) {
        if (omsOrderId == 0) return;
        OmsOrder order = lifecycleManager.applyFill(omsOrderId, clusterLegId, quantity);
        if (order == null) return;
        touched.add(order);
        if (order.getOrderType() == OmsOrderType.ICEBERG) {
            long remaining = Math.max(0, order.getSliceRemainingQty() - quantity);
            order.setSliceRemainingQty(remaining);
            if (remaining == 0) exhausted.add(omsOrderId);
        }
    }

    public boolean isDurableStateHealthy() { return durableStateHealthy; }
    public void markDurableStateFailed() { durableStateHealthy = false; }

    /** A persistence error is latched until an authoritative restart/recovery. */
    public void persistOrderState(OmsOrder order) {
        if (!durableStateHealthy) throw new IllegalStateException("Durable order recovery required");
        try {
            if (persistenceHandler != null) persistenceHandler.persistOrderUpdate(order);
        } catch (RuntimeException e) {
            durableStateHealthy = false;
            throw e;
        }
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
     * @param egressSeq    Layer 2: order key threaded for future reorder handling; adapter tracks
     *                     the reorder metric. Stored nowhere yet.
     */
    public void onClusterOrderStatus(int marketId, long clusterOrderId, long userId, int status,
                                      long price, long remainingQty, long filledQty,
                                      boolean isBuy, long omsOrderId, String rejectReason,
                                      long egressSeq) {
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
            persistOrderState(order);
        }
    }

    /**
     * Process TradeExecutionBatch entry from cluster egress.
     * Called on OMS Core Thread via Disruptor.
     *
     * @param egressSeq Layer 2: order key threaded for future reorder handling; adapter tracks
     *                  the reorder metric. Stored nowhere yet.
     */
    public void onTradeExecution(int marketId, long tradeId, long takerOrderId, long makerOrderId,
                                  long takerUserId, long makerUserId, long tradePrice,
                                  long tradeQuantity, boolean takerIsBuy,
                                  long takerOmsOrderId, long makerOmsOrderId,
                                  long egressSeq) {
        if (journalAuthoritative) {
            // Cutover mode: the journal consumer applies trades once, from committed history.
            return;
        }
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
        // oms#110: pass each side's ME-assigned cluster leg id (taker's = takerOrderId, maker's =
        // makerOrderId) so an order filling in the same batch as its accept records its real
        // clusterOrderId here, BEFORE the trade-driven persist below — otherwise it persists as 0.
        OmsOrder takerOrder = takerOmsOrderId != 0
                ? lifecycleManager.applyFill(takerOmsOrderId, takerOrderId, tradeQuantity) : null;
        OmsOrder makerOrder = makerOmsOrderId != 0
                ? lifecycleManager.applyFill(makerOmsOrderId, makerOrderId, tradeQuantity) : null;

        if (persistenceHandler != null) {
            if (takerOrder != null) persistOrderState(takerOrder);
            if (makerOrder != null) persistOrderState(makerOrder);
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
        synchronized (order) {
            long remaining = Math.max(0, order.getSliceRemainingQty() - tradeQuantity);
            order.setSliceRemainingQty(remaining);
            persistOrderState(order);
            if (remaining == 0) {
                // Slice exhausted: the synthetic engine submits the next slice via
                // submitIcebergSlice (re-arming the counter), or self-cleans when the
                // hidden remainder is gone (the parent just went FILLED via applyFill).
                syntheticEngine.onIcebergSliceFilled(omsOrderId);
            }
        }
    }

    /**
     * Process market data update from egress.
     * Updates synthetic order engine for stop/trailing evaluation.
     */
    public void onMarketDataUpdate(int marketId, long bestBid, long bestAsk) {
        if (!durableStateHealthy) throw new IllegalStateException("Durable order recovery required");
        syntheticEngine.onMarketDataUpdate(marketId, bestBid, bestAsk);
    }

    // ==================== Synthetic Order Handling ====================

    private void onSyntheticTriggered(OmsOrder parentOrder, OmsOrderType childType, long childPrice) {
        synchronized (parentOrder) {
            if (parentOrder.isCancelRequested() || parentOrder.getStatus() != OmsOrderStatus.PENDING_TRIGGER) return;
            // Persist disarming before send. An uncertain send stays PENDING_NEW
            // across restart; re-arming could produce a second ME child.
            parentOrder.setStatus(OmsOrderStatus.PENDING_NEW);
            parentOrder.setUpdatedAtMs(System.currentTimeMillis());
            persistOrderState(parentOrder);
            if (clusterSubmitHandler == null
                    || !clusterSubmitHandler.submitTriggeredOrder(parentOrder, childType, childPrice)) {
                unresolvedOrderIds.add(parentOrder.getOmsOrderId());
            }
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
        synchronized (icebergOrder) {
            if (icebergOrder.isCancelRequested() || icebergOrder.isTerminal()) return false;
            icebergOrder.setSliceRemainingQty(sliceQuantity);
            persistOrderState(icebergOrder);
            boolean enqueued = clusterSubmitHandler != null
                    && clusterSubmitHandler.submitIcebergSlice(icebergOrder, sliceQuantity);
            if (!enqueued) unresolvedOrderIds.add(icebergOrder.getOmsOrderId());
            return enqueued;
        }
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

        // A lost acknowledgement leaves the replace outcome UNKNOWN. A timeout cannot
        // authorize rolling back the extra hold: the new leg may already be live.
        java.util.concurrent.atomic.AtomicBoolean timedOutReplace = new java.util.concurrent.atomic.AtomicBoolean();
        lifecycleManager.forEachActiveOrder(order -> {
            if (order.isReplacePending()
                    && nowMs - order.getReplaceRequestedAtMs() > REPLACE_PENDING_TIMEOUT_MS) {
                unresolvedOrderIds.add(order.getOmsOrderId());
                timedOutReplace.set(true);
            }
        });
        if (timedOutReplace.get()) {
            requestOpenOrdersSnapshot(nowMs, "replace outcome requires recovery");
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
            OmsOrder order = lifecycleManager.getOrder(omsOrderId);
            if (order == null) continue;
            synchronized (order) {
                if (order.getStatus() == OmsOrderStatus.PENDING_TRIGGER) {
                    // Dormant synthetic parent: no ME leg has been submitted.
                    OmsOrder expired = lifecycleManager.onExpired(omsOrderId);
                    if (expired != null && persistenceHandler != null) persistOrderState(expired);
                } else {
                    // Expiry requests cancellation; only the authoritative outcome closes a
                    // submitted order. In particular do not unlock while the cancel is in flight.
                    lifecycleManager.onCancelRequested(omsOrderId);
                    persistOrderState(order);
                    if (order.getClusterOrderId() != 0 && clusterSubmitHandler != null) {
                        clusterSubmitHandler.submitCancel(order.getClusterOrderId(), order.getUserId(), order.getMarketId());
                    } else {
                        unresolvedOrderIds.add(omsOrderId);
                        requestOpenOrdersSnapshot(nowMs, "GTD awaiting cluster order identity");
                    }
                }
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

    /** Request a fresh positive membership view; it cannot supply terminal history. */
    public void requestOpenOrdersSnapshot(long requestId, String reason) {
        if (clusterSubmitHandler != null) {
            log.info("Requesting open-orders snapshot: requestId={} reason={}", requestId, reason);
            clusterSubmitHandler.submitOpenOrdersSnapshotRequest(requestId);
        }
    }

    /**
     * Positive membership can restore a lost cluster-id link. Absence has no terminal
     * semantics: a filled order and a cancelled order are both absent. Such orders
     * remain unresolved until their durable outcome is replayed; age is not proof.
     */
    public int reconcileAgainstOpenOrders(org.agrona.collections.LongHashSet clusterOpenOrderIds,
                                          org.agrona.collections.Long2LongHashMap clusterOmsToClusterId,
                                          long snapshotMaxOrderId, long requestTimeMs) {
        final org.agrona.collections.LongHashSet openOms =
                new org.agrona.collections.LongHashSet(clusterOmsToClusterId.size());
        for (final long id : clusterOmsToClusterId.keySet()) openOms.add(id);
        lastClusterOpenOmsOrderIds = openOms;
        ArrayList<OmsOrder> toRelink = new ArrayList<>();
        lifecycleManager.forEachActiveOrder(order -> {
            OmsOrderStatus status = order.getStatus();
            if (status == OmsOrderStatus.PENDING_TRIGGER || status == OmsOrderStatus.PENDING_RISK
                    || status == OmsOrderStatus.PENDING_HOLD) return;
            long cid = order.getClusterOrderId();
            if (clusterOmsToClusterId.containsKey(order.getOmsOrderId())) {
                long observedCid = clusterOmsToClusterId.get(order.getOmsOrderId());
                if (cid == 0 || (order.isReplacePending() && observedCid != cid)) {
                    toRelink.add(order);
                }
                if (!order.isReplacePending() || observedCid != cid) unresolvedOrderIds.remove(order.getOmsOrderId());
            } else if ((cid != 0 && cid < snapshotMaxOrderId && !clusterOpenOrderIds.contains(cid))
                    || (cid == 0 && requestTimeMs - order.getCreatedAtMs() > ORPHAN_MIN_AGE_MS)) {
                // An open-order snapshot has no terminal history. Keep the order and hold
                // intact until durable trade/terminal recovery resolves this absence.
                unresolvedOrderIds.add(order.getOmsOrderId());
            }
        });
        for (OmsOrder order : toRelink) {
            long cid = clusterOmsToClusterId.get(order.getOmsOrderId());
            if (order.isReplacePending()) {
                lifecycleManager.resolveReplaceFromReconcile(order.getOmsOrderId(), cid);
            } else {
                lifecycleManager.onSentToCluster(order.getOmsOrderId(), cid);
            }
            if (persistenceHandler != null) persistOrderState(order);
        }
        totalRelinkedOrders += toRelink.size();
        if (postReconcileHook != null) postReconcileHook.run();
        return 0; // No terminal state was invented from negative evidence.
    }

    private final java.util.Set<Long> unresolvedOrderIds = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** Admission stays closed while an order's external outcome is unknown. */
    public void markUnresolved(long omsOrderId) { unresolvedOrderIds.add(omsOrderId); }

    public void clearUnresolved(long omsOrderId) { unresolvedOrderIds.remove(omsOrderId); }

    public boolean isUnresolved(long omsOrderId) { return unresolvedOrderIds.contains(omsOrderId); }

    public int getUnresolvedOrderCount() {
        unresolvedOrderIds.removeIf(id -> lifecycleManager.getOrder(id) == null);
        return unresolvedOrderIds.size();
    }

    /** Cluster-open omsOrderIds from the LAST membership reconcile (volatile immutable copy). */
    private volatile org.agrona.collections.LongHashSet lastClusterOpenOmsOrderIds =
            new org.agrona.collections.LongHashSet(0);

    /** TRUE when the last ME open-orders snapshot listed this omsOrderId as open on the cluster. */
    public boolean isClusterOpenOmsOrderId(long omsOrderId) {
        return lastClusterOpenOmsOrderIds.contains(omsOrderId);
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
            persistOrderState(order);
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
        boolean submitTriggeredOrder(OmsOrder parentOrder, OmsOrderType childType, long childPrice);
        /** @return false when the cluster ingress queue was full (the slice was NOT enqueued). */
        boolean submitIcebergSlice(OmsOrder icebergOrder, long sliceQuantity);
        void submitCancel(long clusterOrderId, long userId, int marketId);
        /** match#31: ask the cluster for an OpenOrdersSnapshot egress. */
        void submitOpenOrdersSnapshotRequest(long requestId);
    }
}
