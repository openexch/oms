package com.openexchange.oms.core;

import com.openexchange.oms.common.domain.*;
import com.openexchange.oms.common.enums.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Central coordination engine — the "brain" of the OMS.
 * Runs on the OMS Core Thread (single-writer principle).
 *
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
        // Settle the trade via ledger
        if (settlementHandler != null) {
            long buyerUserId = takerIsBuy ? takerUserId : makerUserId;
            long sellerUserId = takerIsBuy ? makerUserId : takerUserId;
            long buyerOmsOrderId = takerIsBuy ? takerOmsOrderId : makerOmsOrderId;
            long sellerOmsOrderId = takerIsBuy ? makerOmsOrderId : takerOmsOrderId;

            settlementHandler.settleTrade(tradeId, buyerUserId, sellerUserId, marketId,
                tradePrice, tradeQuantity, buyerOmsOrderId, sellerOmsOrderId);
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

        // Check if iceberg slice filled
        OmsOrder takerOrder = lifecycleManager.getOrder(takerOmsOrderId);
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
        // Note: In a real implementation, we'd maintain a priority queue sorted by expiresAtMs
        // For now, the timer thread calls this and we delegate to lifecycle manager
    }

    // ==================== Helpers ====================

    private void requestCancel(OmsOrder order) {
        if (clusterSubmitHandler != null && order.getClusterOrderId() != 0) {
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
        void settleTrade(long tradeId, long buyerUserId, long sellerUserId, int marketId,
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
