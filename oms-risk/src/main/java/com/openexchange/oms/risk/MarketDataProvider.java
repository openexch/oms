package com.openexchange.oms.risk;

/**
 * Read-only view of market data for risk checks.
 * <p>
 * Implementations must guarantee that reads are safe for concurrent access from the risk
 * engine thread. Typically the backing fields are volatile or updated via a single-writer
 * pattern from the market data feed.
 * <p>
 * All prices are in fixed-point representation (see {@link com.match.domain.FixedPoint}).
 */
public interface MarketDataProvider {

    /**
     * Last trade price for the given market.
     *
     * @param marketId market identifier
     * @return last trade price in fixed-point, or 0 if no trades have occurred
     */
    long getLastTradePrice(int marketId);

    /**
     * Current best bid price for the given market.
     *
     * @param marketId market identifier
     * @return best bid price in fixed-point, or 0 if no bids exist
     */
    long getBestBid(int marketId);

    /**
     * Current best ask price for the given market.
     *
     * @param marketId market identifier
     * @return best ask price in fixed-point, or 0 if no asks exist
     */
    long getBestAsk(int marketId);
}
