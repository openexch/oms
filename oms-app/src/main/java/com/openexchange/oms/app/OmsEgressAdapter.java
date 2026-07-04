// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.match.infrastructure.generated.BookDeltaDecoder;
import com.match.infrastructure.generated.BookSnapshotDecoder;
import com.match.infrastructure.generated.OpenOrdersSnapshotDecoder;
import com.match.infrastructure.generated.OrderSide;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.LongHashSet;
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

    // ---- P1.2 (oms#34 / match#31): statusSeq gap detection + snapshot repair ----
    // All egress callbacks run on the single Aeron polling thread; no locking needed.

    /** Per-market last seen statusSeq; MISSING (-1) until first seq after (re)baseline. */
    private final Long2LongHashMap lastStatusSeq = new Long2LongHashMap(-1L);
    private long lastTradeId;
    private long statusGapCount;
    private long tradeGapCount;
    private long lastResnapshotRequestMs;
    private static final long RESNAPSHOT_MIN_INTERVAL_MS = 5_000;

    /** Accumulates OpenOrdersSnapshot chunks for the active requestId. */
    private final LongHashSet snapshotOrderIds = new LongHashSet();
    /** omsOrderId → clusterOrderId of cluster-open orders, for orphan repair + re-linking (oms#53). */
    private final Long2LongHashMap snapshotOmsToClusterId = new Long2LongHashMap(0L);
    private long snapshotRequestId;
    private long snapshotRequestTimeMs;

    public OmsEgressAdapter(OmsCoreEngine coreEngine, OmsMarketDataProvider marketDataProvider) {
        this.coreEngine = coreEngine;
        this.marketDataProvider = marketDataProvider;
    }

    public boolean isConnected() {
        return connected;
    }

    /** Cumulative OrderStatus seq-gap count (statuses shed on the wire) — for /metrics. */
    public long getStatusGapCount() {
        return statusGapCount;
    }

    /** Cumulative tradeId gap count (TradeExecutions shed on the wire) — for /metrics. */
    public long getTradeGapCount() {
        return tradeGapCount;
    }

    @Override
    public void onTradeExecution(int marketId, long tradeId, long takerOrderId, long makerOrderId,
                                  long takerUserId, long makerUserId, long price, long quantity,
                                  boolean takerIsBuy, long takerOmsOrderId, long makerOmsOrderId) {
        marketDataProvider.updateLastTrade(marketId, price);
        // tradeIds are globally monotonic and gap-free from the engine: a gap on
        // the wire means TradeExecutions were shed. Fills cannot be conjured back,
        // but terminality can converge — trigger the membership repair (match#31).
        if (lastTradeId > 0 && tradeId > lastTradeId + 1) {
            tradeGapCount += tradeId - lastTradeId - 1;
            log.warn("TradeExecution gap: lastTradeId={} received={} (missing {}); requesting open-orders snapshot",
                    lastTradeId, tradeId, tradeId - lastTradeId - 1);
            requestOpenOrdersResnapshot("tradeId gap");
        }
        if (tradeId > lastTradeId) {
            lastTradeId = tradeId;
        }
        coreEngine.onTradeExecution(marketId, tradeId, takerOrderId, makerOrderId,
                takerUserId, makerUserId, price, quantity, takerIsBuy,
                takerOmsOrderId, makerOmsOrderId);
    }

    @Override
    public void onOrderStatusUpdate(int marketId, long orderId, long userId, int status,
                                     long price, long remainingQty, long filledQty,
                                     boolean isBuy, long omsOrderId, long statusSeq) {
        // Per-market contiguity: the publisher consumes a seq for every status,
        // dropped or not, so any wire gap means statuses were lost (match#31).
        // Seq counters reset on leader change; onConnected/onReconnected clears
        // the baselines so the first seq after a seam is adopted, not compared.
        if (statusSeq > 0) {
            final long last = lastStatusSeq.get(marketId);
            if (last != -1L && statusSeq > last + 1) {
                statusGapCount += statusSeq - last - 1;
                log.warn("OrderStatus seq gap: market={} last={} received={} (missing {}); requesting open-orders snapshot",
                        marketId, last, statusSeq, statusSeq - last - 1);
                requestOpenOrdersResnapshot("statusSeq gap");
            }
            if (last == -1L || statusSeq > last) {
                lastStatusSeq.put(marketId, statusSeq);
            }
        }
        coreEngine.onClusterOrderStatus(marketId, orderId, userId, status,
                price, remainingQty, filledQty, isBuy, omsOrderId);
    }

    @Override
    public void onOpenOrdersSnapshot(OpenOrdersSnapshotDecoder decoder) {
        final long requestId = decoder.requestId();
        final long maxOrderId = decoder.snapshotMaxOrderId();
        if (requestId != snapshotRequestId) {
            // New snapshot stream (or one we did not ask for): start accumulating fresh.
            snapshotRequestId = requestId;
            snapshotOrderIds.clear();
            snapshotOmsToClusterId.clear();
        }
        for (OpenOrdersSnapshotDecoder.OrdersDecoder order : decoder.orders()) {
            snapshotOrderIds.add(order.orderId());
            if (order.omsOrderId() != 0) {
                snapshotOmsToClusterId.put(order.omsOrderId(), order.orderId());
            }
        }
        if (decoder.isLast() == 1) {
            log.info("OpenOrdersSnapshot complete: requestId={} openOrders={} maxOrderId={}",
                    requestId, snapshotOrderIds.size(), maxOrderId);
            coreEngine.reconcileAgainstOpenOrders(snapshotOrderIds, snapshotOmsToClusterId,
                    maxOrderId, snapshotRequestTimeMs);
            snapshotOrderIds.clear();
            snapshotOmsToClusterId.clear();
            snapshotRequestId = 0;
        }
    }

    /**
     * Stale-orphan sweep (oms#41): called from the 1s timer thread. When a
     * submitted order has outlived the orphan age gate without a
     * clusterOrderId (create/ack lost at a seam) and nothing else will
     * trigger a reconcile (quiet cluster: no seq gaps, no reconnects),
     * request one. Rate-limited by the shared resnapshot limiter and
     * self-quenching: the reconcile terminalizes or re-links the orphans.
     */
    public void sweepStaleOrphans() {
        if (isConnected() && coreEngine.hasStaleSubmittedOrphans(System.currentTimeMillis())) {
            requestOpenOrdersResnapshot("stale-orphan sweep");
        }
    }

    private void requestOpenOrdersResnapshot(String reason) {
        final long now = System.currentTimeMillis();
        if (now - lastResnapshotRequestMs < RESNAPSHOT_MIN_INTERVAL_MS) {
            return; // rate-limited; the pending/next snapshot covers this gap too
        }
        lastResnapshotRequestMs = now;
        snapshotRequestTimeMs = now;
        coreEngine.requestOpenOrdersSnapshot(now, reason);
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
        // Reconcile any orders whose cancel was lost across a disconnect/reconnect (oms#21).
        coreEngine.requestReconcile(System.currentTimeMillis());
        rebaselineAndRepair();
    }

    @Override
    public void onReconnected() {
        // Leader switchover (session stayed up). Reconcile pending cancels lost at the seam (oms#21).
        coreEngine.requestReconcile(System.currentTimeMillis());
        rebaselineAndRepair();
    }

    /** After any session seam the seq space is new (leader-local counters): adopt
     *  fresh baselines and request the membership snapshot unconditionally (match#31). */
    private void rebaselineAndRepair() {
        lastStatusSeq.clear();
        lastTradeId = 0;
        lastResnapshotRequestMs = 0; // seams always repair, regardless of rate limit
        requestOpenOrdersResnapshot("session seam");
    }

    @Override
    public void onDisconnected() {
        connected = false;
        log.warn("Cluster connection lost");
    }
}
