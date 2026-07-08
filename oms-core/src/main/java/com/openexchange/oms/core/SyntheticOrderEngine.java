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
        int marketId = order.getMarketId();
        synchronized (lock) {
            switch (order.getOrderType()) {
                case STOP_LOSS:
                case STOP_LIMIT:
                    registerStopOrder(order, marketId);
                    break;
                case TRAILING_STOP:
                    registerTrailingOrder(order);
                    break;
                case ICEBERG:
                    icebergOrders.put(order.getOmsOrderId(), order);
                    break;
                default:
                    break;
            }
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
        // Initialize arm price to current market price
        if (order.getSide() == OrderSide.SELL) {
            order.setTrailingArmPrice(bestBid[order.getMarketId()]);
        } else {
            order.setTrailingArmPrice(bestAsk[order.getMarketId()]);
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
        long nextSlice;
        synchronized (lock) {
            iceberg = icebergOrders.get(omsOrderId);
            if (iceberg == null) return;

            // OMS-11 / OMS-6: never resubmit a slice for an iceberg the user has cancelled (or that
            // has otherwise terminalized). A slice-fill event can race a user cancel; refilling here
            // would resurrect an order the user asked to cancel AND re-link a fresh cluster order id,
            // which then makes the pending CANCELLED egress look stale and get swallowed. Dropping the
            // tracking here (cancelRequested is volatile) closes both windows: no new slice, and the
            // CANCELLED for the still-current slice terminalizes normally.
            if (iceberg.isCancelRequested() || iceberg.getStatus().isTerminal()) {
                icebergOrders.remove(omsOrderId);
                return;
            }

            long hiddenRemaining = iceberg.getHiddenQuantity() - iceberg.getDisplayQuantity();
            if (hiddenRemaining <= 0) {
                // All slices filled
                icebergOrders.remove(omsOrderId);
                return;
            }

            iceberg.setHiddenQuantity(hiddenRemaining);
            nextSlice = Math.min(iceberg.getDisplayQuantity(), hiddenRemaining);
        }

        // Submit the next slice OUTSIDE the lock (it enqueues to the cluster).
        if (icebergCallback != null) {
            icebergCallback.onSliceFilled(iceberg, nextSlice);
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

        // Update arm prices + detach the triggered orders under the lock; fire the trigger
        // callbacks AFTER releasing it (the callback submits a child order to the cluster).
        List<OmsOrder> toTrigger = null;
        synchronized (lock) {
            List<Long> triggeredIds = null;
            Long2ObjectHashMap<OmsOrder>.ValueIterator iter = trailingOrders.values().iterator();
            while (iter.hasNext()) {
                OmsOrder order = iter.next();
                if (order.getMarketId() != marketId) continue;
                if (order.getStatus() != OmsOrderStatus.PENDING_TRIGGER) continue;

                long delta = order.getTrailingDelta();

                if (order.getSide() == OrderSide.SELL) {
                    // Track highest bid, trigger when bid falls by delta from high
                    if (bid > order.getTrailingArmPrice()) {
                        order.setTrailingArmPrice(bid);
                    } else if (order.getTrailingArmPrice() - bid >= delta) {
                        if (triggeredIds == null) triggeredIds = new ArrayList<>();
                        triggeredIds.add(order.getOmsOrderId());
                    }
                } else {
                    // Track lowest ask, trigger when ask rises by delta from low
                    if (ask < order.getTrailingArmPrice() || order.getTrailingArmPrice() == 0) {
                        order.setTrailingArmPrice(ask);
                    } else if (ask - order.getTrailingArmPrice() >= delta) {
                        if (triggeredIds == null) triggeredIds = new ArrayList<>();
                        triggeredIds.add(order.getOmsOrderId());
                    }
                }
            }

            if (triggeredIds != null) {
                for (long id : triggeredIds) {
                    OmsOrder order = trailingOrders.remove(id);
                    if (order != null) {
                        if (toTrigger == null) toTrigger = new ArrayList<>();
                        toTrigger.add(order);
                    }
                }
            }
        }

        if (toTrigger != null) {
            for (OmsOrder order : toTrigger) {
                log.info("Trailing stop triggered: omsOrderId={}, armPrice={}, delta={}",
                    order.getOmsOrderId(), order.getTrailingArmPrice(), order.getTrailingDelta());
                triggerCallback.onTrigger(order, OmsOrderType.MARKET, 0);
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
