package com.openexchange.oms.persistence;

/**
 * Net position for one user in one market, aggregated from the persisted
 * executions ledger (BUY quantities minus SELL quantities). Startup state
 * rebuild source for RiskEngine positions (oms#35).
 */
public record PositionAggregate(long userId, int marketId, long netQuantity) {
}
