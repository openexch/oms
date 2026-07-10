// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.ledger;

/**
 * In-memory balance store backed by Redis.
 * <p>
 * Per-user, per-asset balances are tracked as two components:
 * <ul>
 *   <li><b>available</b> - funds free for new orders or withdrawals</li>
 *   <li><b>locked</b> - funds reserved by open orders</li>
 * </ul>
 * All amounts use fixed-point representation with 8 decimal places (long values).
 */
public interface BalanceStore {

    /**
     * Returns the available balance for a user/asset pair.
     *
     * @param userId  user identifier
     * @param assetId asset identifier (see {@link com.openexchange.oms.common.enums.Asset})
     * @return available balance in fixed-point 8-decimal representation, or 0 if no balance exists
     */
    long getAvailable(long userId, int assetId);

    /**
     * Returns the locked (held) balance for a user/asset pair.
     *
     * @param userId  user identifier
     * @param assetId asset identifier
     * @return locked balance in fixed-point 8-decimal representation, or 0 if no balance exists
     */
    long getLocked(long userId, int assetId);

    /**
     * Atomically holds funds for an order: decrements available balance, increments locked balance.
     * Fails if the available balance is insufficient.
     *
     * @param userId  user identifier
     * @param assetId asset identifier
     * @param amount  amount to hold (must be positive)
     * @param orderId order identifier for audit trail
     * @return true if hold was successful, false if insufficient available balance
     */
    boolean hold(long userId, int assetId, long amount, long orderId);

    /**
     * Hold with a release-ownership policy. {@code omsManagedRelease=true} marks the hold as
     * OMS-owned at terminal (iceberg/stop PARENT holds: their ME-level slices/children share the
     * parent's omsOrderId, so a feed terminal must not release the parent's residual). Stores
     * without per-hold policy (Redis/in-memory) ignore the flag — their releases are always
     * OMS-computed anyway.
     */
    default boolean hold(long userId, int assetId, long amount, long orderId, boolean omsManagedRelease) {
        return hold(userId, assetId, amount, orderId);
    }

    /**
     * Atomically releases a hold: decrements locked balance, increments available balance.
     * Used when an order is cancelled or partially unfilled.
     *
     * @param userId  user identifier
     * @param assetId asset identifier
     * @param amount  amount to release (must be positive)
     * @param orderId order identifier for audit trail
     * @return true if release was successful, false if insufficient locked balance
     */
    boolean release(long userId, int assetId, long amount, long orderId);

    /**
     * Atomically settles a trade between buyer and seller.
     * <p>
     * Buyer side: locked quoteAsset decremented by quoteAmount, available baseAsset incremented by baseAmount.
     * Seller side: locked baseAsset decremented by baseAmount, available quoteAsset incremented by quoteAmount.
     * <p>
     * Idempotent on tradeId -- repeated calls with the same tradeId are no-ops.
     *
     * @param buyerUserId  buyer's user identifier
     * @param sellerUserId seller's user identifier
     * @param baseAssetId  base asset identifier (e.g. BTC in BTC-USD)
     * @param quoteAssetId quote asset identifier (e.g. USD in BTC-USD)
     * @param baseAmount   base asset amount (quantity)
     * @param quoteAmount  quote asset amount (price * quantity)
     * @param tradeId      unique trade identifier for idempotency
     * @return true if the trade was newly applied; false if it was a duplicate (already-seen tradeId)
     */
    boolean settle(long buyerUserId, long sellerUserId, int baseAssetId, int quoteAssetId,
                long baseAmount, long quoteAmount, long tradeId);

    /**
     * Returns the number of over-settle events observed by this store since creation.
     * <p>
     * An over-settle is a {@link #settle} in which a locked balance held less than the
     * trade required, forcing a floor-guarded clamp (see oms#84). Each such settlement
     * is counted once regardless of how many legs were affected. A nonzero and rising
     * value signals an upstream accounting-invariant break (e.g. an amend that grew
     * notional without an incremental hold) and is wired to the
     * {@code oms_ledger_oversettle_total} metric.
     *
     * @return count of over-settle events since store creation
     */
    default long getOversettleCount() {
        return 0;
    }

    /**
     * Whether this store tracks per-order RESIDUAL holds authoritatively (the Assets Engine).
     * When true: terminal releases use {@link #releaseAll} (the residual model is truth, and buy
     * price-improvement returns with it) and LedgerService skips the per-fill overlock release.
     */
    default boolean supportsResidualHolds() {
        return false;
    }

    /**
     * Release the FULL residual of an order's hold (only meaningful when
     * {@link #supportsResidualHolds()}); amount-computing stores never see this call.
     */
    default boolean releaseAll(long userId, int assetId, long orderId) {
        throw new UnsupportedOperationException("residual holds not supported by " + getClass().getSimpleName());
    }

    /**
     * Deposits funds into a user's available balance.
     *
     * @param userId  user identifier
     * @param assetId asset identifier
     * @param amount  amount to deposit (must be positive)
     */
    void deposit(long userId, int assetId, long amount);

    /**
     * Withdraws funds from a user's available balance.
     *
     * @param userId  user identifier
     * @param assetId asset identifier
     * @param amount  amount to withdraw (must be positive)
     * @throws IllegalStateException if available balance is insufficient
     */
    void withdraw(long userId, int assetId, long amount);
}
