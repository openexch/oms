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

    /**
     * Called when market data updates arrive.
     * Evaluates all synthetic triggers for the given market.
     */
    public void onMarketDataUpdate(int marketId, long newBestBid, long newBestAsk) {
        bestBid[marketId] = newBestBid;
        bestAsk[marketId] = newBestAsk;

        evaluateStopOrders(marketId, newBestBid, newBestAsk);
        evaluateTrailingStops(marketId, newBestBid, newBestAsk);
    }

    /**
     * Called when an iceberg slice is filled.
     */
    public void onIcebergSliceFilled(long omsOrderId) {
        OmsOrder iceberg = icebergOrders.get(omsOrderId);
        if (iceberg == null) return;

        long hiddenRemaining = iceberg.getHiddenQuantity() - iceberg.getDisplayQuantity();
        if (hiddenRemaining <= 0) {
            // All slices filled
            icebergOrders.remove(omsOrderId);
            return;
        }

        iceberg.setHiddenQuantity(hiddenRemaining);
        long nextSlice = Math.min(iceberg.getDisplayQuantity(), hiddenRemaining);

        if (icebergCallback != null) {
            icebergCallback.onSliceFilled(iceberg, nextSlice);
        }
    }

    private void evaluateStopOrders(int marketId, long bid, long ask) {
        if (triggerCallback == null) return;

        // Sell stops: trigger when bestBid <= stopPrice
        TreeMap<Long, List<OmsOrder>> sells = sellStops.get(marketId);
        if (sells != null && !sells.isEmpty() && bid > 0) {
            // All entries where stopPrice >= bid should trigger
            var triggered = sells.tailMap(bid, true);
            List<OmsOrder> toTrigger = new ArrayList<>();
            for (List<OmsOrder> orders : triggered.values()) {
                toTrigger.addAll(orders);
            }
            triggered.clear();

            for (OmsOrder order : toTrigger) {
                triggerStopOrder(order);
            }
        }

        // Buy stops: trigger when bestAsk >= stopPrice
        TreeMap<Long, List<OmsOrder>> buys = buyStops.get(marketId);
        if (buys != null && !buys.isEmpty() && ask > 0) {
            // All entries where stopPrice <= ask should trigger
            var triggered = buys.headMap(ask, true);
            List<OmsOrder> toTrigger = new ArrayList<>();
            for (List<OmsOrder> orders : triggered.values()) {
                toTrigger.addAll(orders);
            }
            triggered.clear();

            for (OmsOrder order : toTrigger) {
                triggerStopOrder(order);
            }
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
                    log.info("Trailing stop triggered: omsOrderId={}, armPrice={}, delta={}",
                        order.getOmsOrderId(), order.getTrailingArmPrice(), order.getTrailingDelta());
                    triggerCallback.onTrigger(order, OmsOrderType.MARKET, 0);
                }
            }
        }
    }

    public int getActiveStopCount() {
        int count = 0;
        for (TreeMap<Long, List<OmsOrder>> map : sellStops.values()) {
            for (List<OmsOrder> list : map.values()) count += list.size();
        }
        for (TreeMap<Long, List<OmsOrder>> map : buyStops.values()) {
            for (List<OmsOrder> list : map.values()) count += list.size();
        }
        return count;
    }

    public int getActiveTrailingCount() {
        return trailingOrders.size();
    }

    public int getActiveIcebergCount() {
        return icebergOrders.size();
    }
}
