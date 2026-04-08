package com.openexchange.oms.app;

import com.openexchange.oms.risk.MarketDataProvider;

/**
 * In-memory market data provider backed by volatile arrays.
 * Updated from the egress path, read by the risk engine.
 */
public class OmsMarketDataProvider implements MarketDataProvider {

    private static final int MAX_MARKETS = 6;

    private volatile long[] lastTradePrices = new long[MAX_MARKETS];
    private volatile long[] bestBids = new long[MAX_MARKETS];
    private volatile long[] bestAsks = new long[MAX_MARKETS];

    public void update(int marketId, long bestBid, long bestAsk) {
        if (marketId < 0 || marketId >= MAX_MARKETS) return;
        bestBids[marketId] = bestBid;
        bestAsks[marketId] = bestAsk;
    }

    public void updateLastTrade(int marketId, long price) {
        if (marketId < 0 || marketId >= MAX_MARKETS) return;
        lastTradePrices[marketId] = price;
    }

    @Override
    public long getLastTradePrice(int marketId) {
        if (marketId < 0 || marketId >= MAX_MARKETS) return 0L;
        return lastTradePrices[marketId];
    }

    @Override
    public long getBestBid(int marketId) {
        if (marketId < 0 || marketId >= MAX_MARKETS) return 0L;
        return bestBids[marketId];
    }

    @Override
    public long getBestAsk(int marketId) {
        if (marketId < 0 || marketId >= MAX_MARKETS) return 0L;
        return bestAsks[marketId];
    }
}
