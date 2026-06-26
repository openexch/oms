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

    public OmsCoreEngine(OrderLifecycleManager lifecycleManager, SyntheticOrderEngine syntheticEngine) {
        this.lifecycleManager = lifecycleManager;
        this.syntheticEngine = syntheticEngine;

        // Wire synthetic trigger callback to create child orders
        syntheticEngine.setTriggerCallback(this::onSyntheticTriggered);
        syntheticEngine.setIcebergCallback(this::onIcebergSliceFilled);
    }

    public void setSettlementHandler(SettlementHandler handler) { this.settlementHandler = handler; }
    public void setPersistenceHandler(PersistenceHandler handler) { this.persistenceHandler = handler; }
    public void setClusterSubmitHandler(ClusterSubmitHandler handler) { this.clusterSubmitHandler = handler; }

    // ==================== Egress Event Processing ====================

    /**
     * Process OrderStatusBatch entry from cluster egress.
     * Called on OMS Core Thread via Disruptor.
     */
    public void onClusterOrderStatus(int marketId, long clusterOrderId, long userId, int status,
                                      long price, long remainingQty, long filledQty,
                                      boolean isBuy, long omsOrderId) {
        OmsOrder order = lifecycleManager.onClusterOrderStatus(omsOrderId, clusterOrderId, status,
            remainingQty, filledQty);

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

        // Check if iceberg slice filled — use the order returned by applyFill, since a FILLED order
        // is removed from the active map (getOrder would now return null).
        if (takerOrder != null && takerOrder.getOrderType() == OmsOrderType.ICEBERG
            && takerOrder.getStatus() == OmsOrderStatus.FILLED) {
            syntheticEngine.onIcebergSliceFilled(takerOmsOrderId);
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

    private void onIcebergSliceFilled(OmsOrder icebergOrder, long nextSliceQuantity) {
        // Submit the next visible slice to the cluster
        if (clusterSubmitHandler != null) {
            clusterSubmitHandler.submitIcebergSlice(icebergOrder, nextSliceQuantity);
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
        void submitIcebergSlice(OmsOrder icebergOrder, long sliceQuantity);
        void submitCancel(long clusterOrderId, long userId, int marketId);
    }
}
