package com.openexchange.oms.risk;

/**
 * Functional interface for balance sufficiency checks.
 * <p>
 * Implementations must be allocation-free on the hot path. The risk engine invokes
 * this once per order to verify that the user has enough available balance (after
 * accounting for existing holds) to cover the new order.
 */
@FunctionalInterface
public interface BalanceChecker {

    /**
     * Check whether the user has sufficient available balance.
     *
     * @param userId  the user placing the order
     * @param assetId the asset that will be debited (quote asset for buys, base asset for sells)
     * @param amount  required amount in fixed-point representation
     * @return {@code true} if balance is sufficient, {@code false} otherwise
     */
    boolean hasSufficientBalance(long userId, int assetId, long amount);
}
