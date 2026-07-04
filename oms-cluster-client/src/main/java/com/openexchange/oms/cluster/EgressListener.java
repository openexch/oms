// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.cluster;

/**
 * Callback interface for egress messages received from the Aeron cluster.
 *
 * <p>All callbacks are invoked on the single-threaded polling loop. Implementations
 * must not block or perform expensive operations in these methods to avoid
 * stalling the Aeron client event loop.</p>
 */
public interface EgressListener {

    /**
     * Called for each individual trade execution within a TradeExecutionBatch.
     *
     * @param marketId       the market where the trade occurred
     * @param tradeId        cluster-assigned trade identifier
     * @param takerOrderId   cluster order ID of the taker
     * @param makerOrderId   cluster order ID of the maker
     * @param takerUserId    user ID of the taker
     * @param makerUserId    user ID of the maker
     * @param price          execution price
     * @param quantity       execution quantity
     * @param takerIsBuy     true if the taker side is BUY
     * @param takerOmsOrderId OMS-assigned order ID of the taker
     * @param makerOmsOrderId OMS-assigned order ID of the maker
     */
    void onTradeExecution(
            int marketId,
            long tradeId,
            long takerOrderId,
            long makerOrderId,
            long takerUserId,
            long makerUserId,
            long price,
            long quantity,
            boolean takerIsBuy,
            long takerOmsOrderId,
            long makerOmsOrderId);

    /**
     * Called for each individual order status update within an OrderStatusBatch.
     *
     * @param marketId     the market the order belongs to
     * @param orderId      cluster-assigned order identifier
     * @param userId       the user who owns the order
     * @param status       order status code (from OrderStatus SBE enum raw value)
     * @param price        order price
     * @param remainingQty remaining unfilled quantity
     * @param filledQty    total filled quantity
     * @param isBuy        true if the order is on the BUY side
     * @param omsOrderId   OMS-assigned order identifier
     * @param statusSeq    per-market monotonic sequence (match#31, schema v3+);
     *                     a gap means the publisher dropped statuses in between.
     *                     0 when the stream predates schema v3.
     */
    void onOrderStatusUpdate(
            int marketId,
            long orderId,
            long userId,
            int status,
            long price,
            long remainingQty,
            long filledQty,
            boolean isBuy,
            long omsOrderId,
            long statusSeq);

    /**
     * Called for each OpenOrdersSnapshot chunk (match#31): the cluster's
     * open-order membership set, requested after a reconnect or a detected
     * statusSeq/tradeId gap. The decoder is valid only during this call.
     * Default no-op.
     */
    default void onOpenOrdersSnapshot(com.match.infrastructure.generated.OpenOrdersSnapshotDecoder decoder) {}

    /**
     * Called when a book delta (incremental update) is received from the cluster.
     * The raw decoder is passed to allow flexible field access without copying.
     *
     * @param marketId the market the delta applies to
     * @param decoder  the SBE decoder positioned at the BookDelta message (valid only during this call)
     */
    void onBookDelta(int marketId, com.match.infrastructure.generated.BookDeltaDecoder decoder);

    /**
     * Called when a full book snapshot is received from the cluster.
     * The raw decoder is passed to allow flexible field access without copying.
     *
     * @param marketId the market the snapshot represents
     * @param decoder  the SBE decoder positioned at the BookSnapshot message (valid only during this call)
     */
    void onBookSnapshot(int marketId, com.match.infrastructure.generated.BookSnapshotDecoder decoder);

    /**
     * Called when the cluster client establishes a connection to the cluster.
     */
    void onConnected();

    /**
     * Called when the cluster client loses its connection to the cluster.
     */
    void onDisconnected();

    /**
     * Called when the cluster session reconnects or the leader changes (switchover). Implementations
     * should reconcile state that could have been lost at the seam (e.g. re-submit pending cancels).
     * Default no-op for implementations that don't need it.
     */
    default void onReconnected() {}
}
