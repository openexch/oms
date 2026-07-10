// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

/**
 * Callback interface for egress events received from the Assets Engine (AE) cluster.
 *
 * <p>Adapted from the matching-engine {@code com.openexchange.oms.cluster.EgressListener}
 * idiom, but carries the money-schema v2 vocabulary (holds, balances, settlement) rather
 * than order/book messages.</p>
 *
 * <h3>Threading</h3>
 * <p>All callbacks are invoked on the single {@code oms-assets-poll} thread. Implementations
 * must not block or perform expensive work inside these methods, or they will stall the
 * Aeron client event loop (which also drives keep-alives and ingress draining).</p>
 *
 * <h3>Amounts</h3>
 * <p>Every {@code amount}/{@code available}/{@code locked}/{@code remaining}/{@code newAvailable}
 * value is an 8-decimal-place fixed-point {@code int64}, matching the money schema.</p>
 */
public interface AssetsEgressListener {

    /**
     * A {@code Hold} was accepted: {@code amount} moved from available to locked for the order.
     *
     * @param correlationId client-chosen id echoed from the originating Hold (0 = fire-and-forget)
     * @param orderId       the order the funds are reserved for
     * @param userId        the account whose funds were reserved
     * @param assetId       the asset that was reserved
     * @param amount        reserved amount (8dp fixed-point)
     */
    void onHoldAck(long correlationId, long orderId, long userId, int assetId, long amount);

    /**
     * A {@code Hold} was rejected; no funds moved.
     *
     * @param correlationId client-chosen id echoed from the originating Hold (0 = fire-and-forget)
     * @param orderId       the order the reservation was attempted for
     * @param userId        the account the reservation was attempted against
     * @param assetId       the asset that would have been reserved
     * @param amount        the requested amount (8dp fixed-point)
     * @param reasonCode    raw {@code RejectReason} code (1=INSUFFICIENT_FUNDS, 2=INVALID_AMOUNT,
     *                      3=UNKNOWN_HOLD)
     */
    void onHoldReject(long correlationId, long orderId, long userId, int assetId, long amount, int reasonCode);

    /**
     * A user's balance for one asset changed. Emitted both for live balance mutations and,
     * during a snapshot, once per {@code (user, asset)} before {@link #onBalanceSnapshotEnd}.
     *
     * @param userId    the account
     * @param assetId   the asset
     * @param available available (unlocked) balance, 8dp fixed-point
     * @param locked    locked (held) balance, 8dp fixed-point
     */
    void onBalanceUpdate(long userId, int assetId, long available, long locked);

    /**
     * A {@code Deposit} was applied (available credited).
     *
     * @param correlationId client-chosen id echoed from the originating Deposit
     * @param userId        the credited account
     * @param assetId       the credited asset
     * @param amount        credited amount (8dp fixed-point)
     * @param newAvailable  available balance after the credit (8dp fixed-point)
     */
    void onDepositAck(long correlationId, long userId, int assetId, long amount, long newAvailable);

    /**
     * A {@code Withdraw} was applied (available debited).
     *
     * @param correlationId client-chosen id echoed from the originating Withdraw
     * @param userId        the debited account
     * @param assetId       the debited asset
     * @param amount        debited amount (8dp fixed-point)
     * @param newAvailable  available balance after the debit (8dp fixed-point)
     */
    void onWithdrawAck(long correlationId, long userId, int assetId, long amount, long newAvailable);

    /**
     * A {@code Withdraw} was rejected; no funds moved.
     *
     * @param correlationId client-chosen id echoed from the originating Withdraw
     * @param userId        the account the debit was attempted against
     * @param assetId       the asset that would have been debited
     * @param amount        the requested amount (8dp fixed-point)
     * @param reasonCode    raw {@code RejectReason} code (1=INSUFFICIENT_FUNDS, 2=INVALID_AMOUNT)
     */
    void onWithdrawReject(long correlationId, long userId, int assetId, long amount, int reasonCode);

    /**
     * A trade was settled (idempotency confirmation from the AE).
     *
     * @param tradeId       the settled trade
     * @param buyerUserId   the buyer account credited/debited
     * @param sellerUserId  the seller account credited/debited
     */
    void onSettlementApplied(long tradeId, long buyerUserId, long sellerUserId);

    /**
     * Answer to {@code QueryFeedPosition}: the ME settlement-journal cursor this AE has consumed.
     *
     * @param correlationId     client-chosen id echoed from the originating query
     * @param consumePosition   the journal position the AE has consumed up to
     * @param lastAppliedTradeId the highest tradeId the AE has settled
     */
    void onFeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId);

    /**
     * Terminates the {@link #onBalanceUpdate} stream answering a {@code RequestBalanceSnapshot}.
     *
     * @param correlationId client-chosen id echoed from the originating request
     * @param entryCount    number of {@code onBalanceUpdate} entries that preceded this terminator
     */
    void onBalanceSnapshotEnd(long correlationId, int entryCount);

    /**
     * One outstanding hold, streamed in answer to a {@code RequestHoldSnapshot}.
     *
     * @param orderId   the order the hold belongs to
     * @param userId    the account holding the funds
     * @param assetId   the held asset
     * @param remaining remaining held amount (8dp fixed-point)
     */
    void onHoldSnapshotEntry(long orderId, long userId, int assetId, long remaining);

    /**
     * Terminates the {@link #onHoldSnapshotEntry} stream answering a {@code RequestHoldSnapshot}.
     *
     * @param correlationId client-chosen id echoed from the originating request
     * @param entryCount    number of {@code onHoldSnapshotEntry} entries that preceded this terminator
     */
    void onHoldSnapshotEnd(long correlationId, int entryCount);

    /**
     * Called when the client establishes a connection to the AE cluster.
     */
    void onConnected();

    /**
     * Called when the client loses its connection to the AE cluster.
     */
    void onDisconnected();

    /**
     * Called on leader change / session reconnect (switchover). Implementations should re-sync
     * money state that could have been lost at the seam (e.g. re-request a balance/hold snapshot).
     * Default no-op for implementations that do not need it.
     */
    default void onReconnected() {}
}
