// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.core;

import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.common.enums.OmsOrderType;
import com.openexchange.oms.common.enums.OrderSide;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Manages synthetic order types: stop-loss, stop-limit, trailing stop, iceberg.
 * Runs on the OMS Core Thread (single-writer).
 *
 * Monitors market data from egress to trigger synthetic orders.
 */
public class SyntheticOrderEngine {

    private static final Logger log = LoggerFactory.getLogger(SyntheticOrderEngine.class);

    /**
     * Callback for when a synthetic order triggers — creates a child order.
     */
    public interface TriggerCallback {
        void onTrigger(OmsOrder parentOrder, OmsOrderType childType, long childPrice);
    }

    /**
     * Callback for iceberg slice completion — submits next slice.
     */
    public interface IcebergSliceCallback {
        void onSliceFilled(OmsOrder icebergOrder, long nextSliceQuantity);
    }

    private java.util.function.Consumer<OmsOrder> checkpointCallback = order -> { };
    private TriggerCallback triggerCallback;
    private IcebergSliceCallback icebergCallback;

    // Per-market sorted structures for stop orders (by stopPrice)
    // TreeMap key = stopPrice, value = list of orders at that price
    private final Int2ObjectHashMap<TreeMap<Long, List<OmsOrder>>> sellStops = new Int2ObjectHashMap<>();
    private final Int2ObjectHashMap<TreeMap<Long, List<OmsOrder>>> buyStops = new Int2ObjectHashMap<>();

    // Trailing stop orders indexed by omsOrderId
    private final Long2ObjectHashMap<OmsOrder> trailingOrders = new Long2ObjectHashMap<>();

    // Iceberg orders indexed by omsOrderId
    private final Long2ObjectHashMap<OmsOrder> icebergOrders = new Long2ObjectHashMap<>();

    // Last known market prices
    private final long[] bestBid = new long[6]; // indexed by marketId (1-5)
    private final long[] bestAsk = new long[6];

    // SYN-1 (oms#70 bug class): despite the "single-writer core thread" intent, register/removeOrder
    // run on Netty I/O threads (createOrder/cancelOrder) while evaluate*/onIcebergSliceFilled run on
    // the OMS core thread. The maps above are NOT thread-safe (Agrona + TreeMap), and concurrent
    // structural modification of an Agrona map corrupts its probe chain into an infinite loop (the
    // twice-seen total REST outage that moved OrderLifecycleManager to ConcurrentHashMap). ALL map
    // access is therefore serialized on this lock. The trigger/iceberg CALLBACKS are invoked OUTSIDE
    // the lock — they submit to the cluster (can block on backpressure) and could re-enter — so the
    // evaluate/refill paths collect the affected orders under the lock, release it, then call out.
    private final Object lock = new Object();

    public void setCheckpointCallback(java.util.function.Consumer<OmsOrder> callback) {
        this.checkpointCallback = java.util.Objects.requireNonNull(callback);
    }

    public void setTriggerCallback(TriggerCallback callback) {
        this.triggerCallback = callback;
    }

    public void setIcebergCallback(IcebergSliceCallback callback) {
        this.icebergCallback = callback;
    }

    /**
     * Register a synthetic order for monitoring.
     */
    public void registerOrder(OmsOrder order) {
        synchronized (order) {
            int marketId = order.getMarketId();
            long previousArm = order.getTrailingArmPrice();
            synchronized (lock) {
                switch (order.getOrderType()) {
                    case STOP_LOSS, STOP_LIMIT -> registerStopOrder(order, marketId);
                    case TRAILING_STOP -> registerTrailingOrder(order);
                    case ICEBERG -> icebergOrders.put(order.getOmsOrderId(), order);
                    default -> { }
                }
            }
            if (previousArm != order.getTrailingArmPrice()) checkpointCallback.accept(order);
        }
    }

    private void registerStopOrder(OmsOrder order, int marketId) {
        if (order.getSide() == OrderSide.SELL) {
            // Sell stop triggers when bestBid <= stopPrice
            sellStops.computeIfAbsent(marketId, k -> new TreeMap<>())
                .computeIfAbsent(order.getStopPrice(), k -> new ArrayList<>())
                .add(order);
        } else {
            // Buy stop triggers when bestAsk >= stopPrice
            buyStops.computeIfAbsent(marketId, k -> new TreeMap<>())
                .computeIfAbsent(order.getStopPrice(), k -> new ArrayList<>())
                .add(order);
        }
    }

    private void registerTrailingOrder(OmsOrder order) {
        // Preserve the durable extreme when restoring an armed order. A new
        // trailing order starts at zero and observes its first valid quote.
        if (order.getTrailingArmPrice() == 0) {
            order.setTrailingArmPrice(order.getSide() == OrderSide.SELL
                    ? bestBid[order.getMarketId()] : bestAsk[order.getMarketId()]);
        }
        trailingOrders.put(order.getOmsOrderId(), order);
    }

    /**
     * Remove a synthetic order (cancelled/expired/filled).
     */
    public void removeOrder(OmsOrder order) {
        long id = order.getOmsOrderId();
        synchronized (lock) {
            trailingOrders.remove(id);
            icebergOrders.remove(id);

            int marketId = order.getMarketId();
            TreeMap<Long, List<OmsOrder>> stopMap = (order.getSide() == OrderSide.SELL)
                ? sellStops.get(marketId) : buyStops.get(marketId);
            if (stopMap != null) {
                List<OmsOrder> ordersAtPrice = stopMap.get(order.getStopPrice());
                if (ordersAtPrice != null) {
                    ordersAtPrice.removeIf(o -> o.getOmsOrderId() == id);
                    if (ordersAtPrice.isEmpty()) {
                        stopMap.remove(order.getStopPrice());
                    }
                }
            }
        }
    }

    /**
     * Called when market data updates arrive.
     * Evaluates all synthetic triggers for the given market.
     */
    public void onMarketDataUpdate(int marketId, long newBestBid, long newBestAsk) {
        synchronized (lock) {
            bestBid[marketId] = newBestBid;
            bestAsk[marketId] = newBestAsk;
        }
        // evaluate* lock internally and fire callbacks after releasing — do NOT hold the lock
        // across them here, or the callback (cluster submit) would run under the lock.
        evaluateStopOrders(marketId, newBestBid, newBestAsk);
        evaluateTrailingStops(marketId, newBestBid, newBestAsk);
    }

    /**
     * Called when an iceberg slice is filled.
     */
    public void onIcebergSliceFilled(long omsOrderId) {
        OmsOrder iceberg;
        synchronized (lock) { iceberg = icebergOrders.get(omsOrderId); }
        if (iceberg == null) return;
        synchronized (iceberg) {
            if (iceberg.isCancelRequested() || iceberg.isTerminal()) {
                synchronized (lock) { icebergOrders.remove(omsOrderId); }
                return;
            }
            long hiddenRemaining = iceberg.getHiddenQuantity() - iceberg.getDisplayQuantity();
            if (hiddenRemaining <= 0) {
                synchronized (lock) { icebergOrders.remove(omsOrderId); }
                return;
            }
            iceberg.setHiddenQuantity(hiddenRemaining);
            long nextSlice = Math.min(iceberg.getDisplayQuantity(), hiddenRemaining);
            if (icebergCallback != null) icebergCallback.onSliceFilled(iceberg, nextSlice);
        }
    }

    private void evaluateStopOrders(int marketId, long bid, long ask) {
        if (triggerCallback == null) return;

        // Collect + detach the triggered stops under the lock, then fire the trigger callbacks
        // AFTER releasing it (the callback submits a child order to the cluster).
        List<OmsOrder> toTrigger = new ArrayList<>();
        synchronized (lock) {
            // Sell stops: trigger when bestBid <= stopPrice (stopPrice >= bid)
            TreeMap<Long, List<OmsOrder>> sells = sellStops.get(marketId);
            if (sells != null && !sells.isEmpty() && bid > 0) {
                var triggered = sells.tailMap(bid, true);
                for (List<OmsOrder> orders : triggered.values()) {
                    toTrigger.addAll(orders);
                }
                triggered.clear();
            }
            // Buy stops: trigger when bestAsk >= stopPrice (stopPrice <= ask)
            TreeMap<Long, List<OmsOrder>> buys = buyStops.get(marketId);
            if (buys != null && !buys.isEmpty() && ask > 0) {
                var triggered = buys.headMap(ask, true);
                for (List<OmsOrder> orders : triggered.values()) {
                    toTrigger.addAll(orders);
                }
                triggered.clear();
            }
        }

        for (OmsOrder order : toTrigger) {
            triggerStopOrder(order);
        }
    }

    private void triggerStopOrder(OmsOrder order) {
        if (order.getStatus() != OmsOrderStatus.PENDING_TRIGGER) return;

        OmsOrderType childType;
        long childPrice;

        if (order.getOrderType() == OmsOrderType.STOP_LOSS) {
            childType = OmsOrderType.MARKET;
            childPrice = 0;
        } else {
            // STOP_LIMIT
            childType = OmsOrderType.LIMIT;
            childPrice = order.getPrice();
        }

        log.info("Stop order triggered: omsOrderId={}, stopPrice={}, childType={}",
            order.getOmsOrderId(), order.getStopPrice(), childType);
        triggerCallback.onTrigger(order, childType, childPrice);
    }

    private void evaluateTrailingStops(int marketId, long bid, long ask) {
        if (triggerCallback == null) return;
        List<OmsOrder> candidates = new ArrayList<>();
        synchronized (lock) {
            trailingOrders.values().forEach(candidates::add);
        }
        for (OmsOrder order : candidates) {
            synchronized (order) {
                if (order.getMarketId() != marketId || order.isCancelRequested()
                        || order.getStatus() != OmsOrderStatus.PENDING_TRIGGER) continue;
                long quote = order.getSide() == OrderSide.SELL ? bid : ask;
                if (quote <= 0) continue; // No quote is not a price move.
                long arm = order.getTrailingArmPrice();
                boolean advance = arm == 0 || (order.getSide() == OrderSide.SELL ? quote > arm : quote < arm);
                if (advance) {
                    order.setTrailingArmPrice(quote);
                    // Checkpoint outside the map lock, before any later trigger.
                    checkpointCallback.accept(order);
                    continue;
                }
                boolean triggered = order.getSide() == OrderSide.SELL
                        ? arm - quote >= order.getTrailingDelta() : quote - arm >= order.getTrailingDelta();
                if (triggered) {
                    synchronized (lock) {
                        if (trailingOrders.remove(order.getOmsOrderId()) == null) continue;
                    }
                    triggerCallback.onTrigger(order, OmsOrderType.MARKET, 0);
                }
            }
        }
    }

    public int getActiveStopCount() {
        synchronized (lock) {
            int count = 0;
            for (TreeMap<Long, List<OmsOrder>> map : sellStops.values()) {
                for (List<OmsOrder> list : map.values()) count += list.size();
            }
            for (TreeMap<Long, List<OmsOrder>> map : buyStops.values()) {
                for (List<OmsOrder> list : map.values()) count += list.size();
            }
            return count;
        }
    }

    public int getActiveTrailingCount() {
        synchronized (lock) {
            return trailingOrders.size();
        }
    }

    public int getActiveIcebergCount() {
        synchronized (lock) {
            return icebergOrders.size();
        }
    }
}
