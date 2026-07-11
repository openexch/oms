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

    // ---- Out-of-order tolerance for the tradeId detector (the 2026-07-11 gap storm) ----
    // tradeId is GLOBAL-dense, but each market flushes its egress on an independent ~20ms timer
    // into one shared queue, so cross-market arrival is routinely a few ids out of global order.
    // A jump past lastTradeId+1 therefore opens PENDING holes rather than counting a gap: the
    // late trade arriving heals its hole (normal, counted in lateHealedCount); only a hole that
    // survives HOLE_PATIENCE_MS is a real loss — counted in tradeGapCount + snapshot repair.
    /** How long a tradeId hole may stay open before it is declared a real gap. */
    private static final long HOLE_PATIENCE_MS = 2_000;
    /** Jump size past which holes are not tracked individually (mass shed / seam): gap immediately. */
    private static final long MAX_TRACKED_HOLE_JUMP = 10_000;
    /** Expiry scan cadence; the map stays tiny (holes usually heal within one flush interval). */
    private static final long HOLE_EXPIRY_CHECK_INTERVAL_MS = 250;
    /** Open holes: missing tradeId -> deadline millis; MISSING (-1) = not a pending hole. */
    private final Long2LongHashMap pendingTradeHoles = new Long2LongHashMap(-1L);
    private long lateHealedCount;
    private long nextHoleExpiryCheckMs;
    private long holePatienceMs = HOLE_PATIENCE_MS;
    private long holeExpiryCheckIntervalMs = HOLE_EXPIRY_CHECK_INTERVAL_MS;

    /** Test hook: shrink the hole-expiry timings so unit tests need no multi-second sleeps. */
    void tuneHoleTrackingForTest(final long patienceMs, final long checkIntervalMs) {
        this.holePatienceMs = patienceMs;
        this.holeExpiryCheckIntervalMs = checkIntervalMs;
    }

    // ---- Layer 2 (match#): egressSeq reorder metric ----
    // egressSeq is the Aeron cluster-log position of the ingress command that produced the event:
    // monotonic non-decreasing in log order, but SPARSE (gaps are normal) and NON-UNIQUE (all
    // events of one command share one value; ties are normal). It is an ORDER KEY ONLY — dense
    // gap detection stays with statusSeq/tradeId above. The flush emits all trades before all
    // statuses of a batch, so an event can arrive carrying a LOWER egressSeq than one already
    // seen; counting those reorders tells us whether a reorder buffer would be warranted.
    // Both counters mirror statusGapCount/tradeGapCount: plain long, mutated only on the single
    // Aeron polling thread (no synchronization needed), read by the metrics scrape via the getters.

    /** Highest egressSeq seen so far in arrival order (0 = none yet / stream is pre-v7). */
    private long lastSeenEgressSeq;
    /** Times a non-zero egressSeq arrived below lastSeenEgressSeq (a wire reorder) — for /metrics. */
    private long reorderCount;

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

    /** Trades that arrived out of order and healed their pending hole (benign interleave) — for /metrics. */
    public long getLateHealedCount() {
        return lateHealedCount;
    }

    /** Currently-open tradeId holes awaiting their late trade — for /metrics. */
    public long getPendingHoleCount() {
        return pendingTradeHoles.size();
    }

    /** Layer 2: times a non-zero egressSeq arrived out of log order (a wire reorder) — for /metrics. */
    public long getEgressReorderCount() {
        return reorderCount;
    }

    /** Layer 2: highest egressSeq seen so far (0 = none / pre-v7 stream) — for /metrics. */
    public long getLastEgressSeq() {
        return lastSeenEgressSeq;
    }

    /**
     * Layer 2 reorder metric. Called for every egress event (trade or status) on the single Aeron
     * polling thread. A non-zero egressSeq below the highest seen is a reorder relative to log order
     * (the flush emits trades before statuses); count it. A non-zero egressSeq at or above the high
     * water mark advances it (equal = a tie, which is normal and does NOT count). Zero is absent
     * (pre-v7 stream / SBE null, normalized upstream): skip entirely — no metric, no advance.
     */
    private void trackEgressSeq(long egressSeq) {
        if (egressSeq == 0) {
            return;
        }
        if (egressSeq < lastSeenEgressSeq) {
            reorderCount++;
        } else {
            lastSeenEgressSeq = egressSeq;
        }
    }

    @Override
    public void onTradeExecution(int marketId, long tradeId, long takerOrderId, long makerOrderId,
                                  long takerUserId, long makerUserId, long price, long quantity,
                                  boolean takerIsBuy, long takerOmsOrderId, long makerOmsOrderId,
                                  long egressSeq) {
        trackEgressSeq(egressSeq); // Layer 2: order-key reorder metric (order-key only; not gap detection)
        marketDataProvider.updateLastTrade(marketId, price);
        // tradeIds are globally monotonic and gap-free FROM THE ENGINE, but not in arrival order:
        // per-market flush timers interleave them by a few ids constantly. So a jump opens
        // pending holes; a hole healed by its late trade is normal, and only a hole that outlives
        // HOLE_PATIENCE_MS means TradeExecutions were truly shed. Fills cannot be conjured back,
        // but terminality can converge — the expiry triggers the membership repair (match#31).
        final long nowMs = System.currentTimeMillis();
        if (pendingTradeHoles.remove(tradeId) != -1L) {
            lateHealedCount++; // an expected out-of-order arrival, not a gap
        } else if (lastTradeId > 0 && tradeId > lastTradeId + 1) {
            final long missing = tradeId - lastTradeId - 1;
            if (missing > MAX_TRACKED_HOLE_JUMP) {
                // Mass shed or a seam artifact: don't build a huge hole map — count and repair now.
                tradeGapCount += missing;
                log.warn("TradeExecution gap (untracked mass jump): lastTradeId={} received={} (missing {}); "
                        + "requesting open-orders snapshot", lastTradeId, tradeId, missing);
                requestOpenOrdersResnapshot("tradeId mass gap");
            } else {
                final long deadline = nowMs + holePatienceMs;
                for (long id = lastTradeId + 1; id < tradeId; id++) {
                    pendingTradeHoles.put(id, deadline);
                }
            }
        }
        if (tradeId > lastTradeId) {
            lastTradeId = tradeId;
        }
        expirePendingHoles(nowMs);
        coreEngine.onTradeExecution(marketId, tradeId, takerOrderId, makerOrderId,
                takerUserId, makerUserId, price, quantity, takerIsBuy,
                takerOmsOrderId, makerOmsOrderId, egressSeq);
    }

    /**
     * Declare every pending hole past its deadline a real gap (count + snapshot repair). Runs at
     * most every {@link #HOLE_EXPIRY_CHECK_INTERVAL_MS} on the polling thread; the map holds the
     * handful of in-flight interleave holes, so the scan is trivially cheap.
     */
    private void expirePendingHoles(final long nowMs) {
        if (nowMs < nextHoleExpiryCheckMs || pendingTradeHoles.isEmpty()) {
            return;
        }
        nextHoleExpiryCheckMs = nowMs + holeExpiryCheckIntervalMs;
        long expired = 0;
        long exampleId = 0;
        final var it = pendingTradeHoles.entrySet().iterator();
        while (it.hasNext()) {
            final var entry = it.next();
            if (entry.getValue() <= nowMs) {
                exampleId = entry.getKey();
                it.remove();
                expired++;
            }
        }
        if (expired > 0) {
            tradeGapCount += expired;
            log.warn("TradeExecution gap: {} hole(s) unhealed after {}ms (e.g. tradeId={}); "
                    + "requesting open-orders snapshot", expired, holePatienceMs, exampleId);
            requestOpenOrdersResnapshot("tradeId gap");
        }
    }

    @Override
    public void onOrderStatusUpdate(int marketId, long orderId, long userId, int status,
                                     long price, long remainingQty, long filledQty,
                                     boolean isBuy, long omsOrderId, long statusSeq,
                                     int rejectReasonRaw, long egressSeq) {
        trackEgressSeq(egressSeq); // Layer 2: order-key reorder metric (order-key only; not gap detection)
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
                price, remainingQty, filledQty, isBuy, omsOrderId,
                mapRejectReason(rejectReasonRaw), egressSeq);
    }

    /**
     * Map the raw engine {@code OrderRejectReason} code (match#75, SBE v6) to the OMS
     * rejectReason string vocabulary. Returns null for the -1 sentinel (no reason available:
     * pre-v6 stream / SBE null) and for NONE (0, carried on every non-reject status), so a reason
     * is only ever attached to an actual reject. Unknown/future codes fall back to
     * {@code ENGINE_REJECT_<n>} rather than throwing, so a newer engine can never break egress
     * dispatch. Names mirror the engine enum so they read alongside the risk-reject vocabulary
     * (RATE_LIMIT_EXCEEDED, etc.).
     */
    static String mapRejectReason(int raw) {
        switch (raw) {
            case -1: // sentinel: pre-v6 stream or SBE null (upstream too old to say)
            case 0:  // NONE, carried on every non-reject status
                return null;
            case 1:  return "PRICE_OUT_OF_RANGE";
            case 2:  return "PRICE_OFF_TICK";
            case 3:  return "LEVEL_FULL";
            case 4:  return "BOOK_FULL";
            case 5:  return "OVERFLOW";
            case 6:  return "INVALID_QUANTITY";
            case 7:  return "MATCH_LIMIT";
            case 8:  return "WOULD_CROSS";
            case 9:  return "NO_LIQUIDITY";
            case 10: return "ORDER_NOT_FOUND";
            default: return "ENGINE_REJECT_" + raw;
        }
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
        pendingTradeHoles.clear(); // holes from the old seam's id space are meaningless now
        lastResnapshotRequestMs = 0; // seams always repair, regardless of rate limit
        requestOpenOrdersResnapshot("session seam");
    }

    @Override
    public void onDisconnected() {
        connected = false;
        log.warn("Cluster connection lost");
    }
}
