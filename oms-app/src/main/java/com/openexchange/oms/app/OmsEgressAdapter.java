package com.openexchange.oms.app;

import com.match.infrastructure.generated.BookDeltaDecoder;
import com.match.infrastructure.generated.BookSnapshotDecoder;
import com.match.infrastructure.generated.OrderSide;
import com.openexchange.oms.cluster.EgressListener;
import com.openexchange.oms.core.OmsCoreEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridges ClusterClient egress callbacks to OmsCoreEngine.
 */
public class OmsEgressAdapter implements EgressListener {

    private static final Logger log = LoggerFactory.getLogger(OmsEgressAdapter.class);

    private final OmsCoreEngine coreEngine;
    private final OmsMarketDataProvider marketDataProvider;
    private volatile boolean connected;

    public OmsEgressAdapter(OmsCoreEngine coreEngine, OmsMarketDataProvider marketDataProvider) {
        this.coreEngine = coreEngine;
        this.marketDataProvider = marketDataProvider;
    }

    public boolean isConnected() {
        return connected;
    }

    @Override
    public void onTradeExecution(int marketId, long tradeId, long takerOrderId, long makerOrderId,
                                  long takerUserId, long makerUserId, long price, long quantity,
                                  boolean takerIsBuy, long takerOmsOrderId, long makerOmsOrderId) {
        marketDataProvider.updateLastTrade(marketId, price);
        coreEngine.onTradeExecution(marketId, tradeId, takerOrderId, makerOrderId,
                takerUserId, makerUserId, price, quantity, takerIsBuy,
                takerOmsOrderId, makerOmsOrderId);
    }

    @Override
    public void onOrderStatusUpdate(int marketId, long orderId, long userId, int status,
                                     long price, long remainingQty, long filledQty,
                                     boolean isBuy, long omsOrderId) {
        coreEngine.onClusterOrderStatus(marketId, orderId, userId, status,
                price, remainingQty, filledQty, isBuy, omsOrderId);
    }

    @Override
    public void onBookDelta(int marketId, BookDeltaDecoder decoder) {
        long bestBid = 0;
        long bestAsk = 0;

        for (BookDeltaDecoder.ChangesDecoder change : decoder.changes()) {
            // Skip removed levels (quantity=0) — they are not valid best prices
            if (change.quantity() == 0) continue;

            if (change.side() == OrderSide.BID) {
                if (change.price() > bestBid) {
                    bestBid = change.price();
                }
            } else if (change.side() == OrderSide.ASK) {
                if (bestAsk == 0 || change.price() < bestAsk) {
                    bestAsk = change.price();
                }
            }
        }

        if (bestBid > 0 || bestAsk > 0) {
            marketDataProvider.update(marketId, bestBid, bestAsk);
            coreEngine.onMarketDataUpdate(marketId, bestBid, bestAsk);
        }
    }

    @Override
    public void onBookSnapshot(int marketId, BookSnapshotDecoder decoder) {
        long bestBid = 0;
        long bestAsk = 0;

        for (BookSnapshotDecoder.BidsDecoder bid : decoder.bids()) {
            if (bid.price() > bestBid) {
                bestBid = bid.price();
            }
        }

        for (BookSnapshotDecoder.AsksDecoder ask : decoder.asks()) {
            if (bestAsk == 0 || ask.price() < bestAsk) {
                bestAsk = ask.price();
            }
        }

        if (bestBid > 0 || bestAsk > 0) {
            marketDataProvider.update(marketId, bestBid, bestAsk);
            coreEngine.onMarketDataUpdate(marketId, bestBid, bestAsk);
        }
    }

    @Override
    public void onConnected() {
        connected = true;
        log.info("Cluster connection established");
    }

    @Override
    public void onDisconnected() {
        connected = false;
        log.warn("Cluster connection lost");
    }
}
