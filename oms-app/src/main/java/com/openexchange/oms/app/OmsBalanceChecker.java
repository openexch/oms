package com.openexchange.oms.app;

import com.openexchange.oms.ledger.BalanceStore;
import com.openexchange.oms.risk.BalanceChecker;

/**
 * Bridges the risk engine's {@link BalanceChecker} to the ledger's {@link BalanceStore}.
 */
public class OmsBalanceChecker implements BalanceChecker {

    private final BalanceStore balanceStore;

    public OmsBalanceChecker(BalanceStore balanceStore) {
        this.balanceStore = balanceStore;
    }

    @Override
    public boolean hasSufficientBalance(long userId, int assetId, long amount) {
        return balanceStore.getAvailable(userId, assetId) >= amount;
    }
}
