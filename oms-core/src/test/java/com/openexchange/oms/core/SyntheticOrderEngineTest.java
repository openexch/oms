package com.openexchange.oms.core;

import com.match.domain.FixedPoint;
import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.common.enums.OmsOrderType;
import com.openexchange.oms.common.enums.OrderSide;
import com.openexchange.oms.common.enums.TimeInForce;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SyntheticOrderEngineTest {

    private SyntheticOrderEngine engine;
    private List<TriggerEvent> triggers;
    private List<IcebergEvent> icebergEvents;

    record TriggerEvent(long omsOrderId, OmsOrderType childType, long childPrice) {}
    record IcebergEvent(long omsOrderId, long nextSliceQty) {}

    @BeforeEach
    void setUp() {
        engine = new SyntheticOrderEngine();
        triggers = new ArrayList<>();
        icebergEvents = new ArrayList<>();

        engine.setTriggerCallback((parent, childType, childPrice) ->
                triggers.add(new TriggerEvent(parent.getOmsOrderId(), childType, childPrice)));
        engine.setIcebergCallback((iceberg, nextSlice) ->
                icebergEvents.add(new IcebergEvent(iceberg.getOmsOrderId(), nextSlice)));
    }

    @Test
    void testStopLossSell() {
        OmsOrder order = createStopOrder(1L, OrderSide.SELL, OmsOrderType.STOP_LOSS,
                FixedPoint.fromDouble(950.0), 0L);
        engine.registerOrder(order);

        // Bid drops to 940 (below stop price 950) → should trigger
        engine.onMarketDataUpdate(1, FixedPoint.fromDouble(940.0), FixedPoint.fromDouble(960.0));

        assertEquals(1, triggers.size());
        assertEquals(OmsOrderType.MARKET, triggers.get(0).childType());
        assertEquals(0L, triggers.get(0).childPrice());
    }

    @Test
    void testStopLossSellNoTrigger() {
        OmsOrder order = createStopOrder(2L, OrderSide.SELL, OmsOrderType.STOP_LOSS,
                FixedPoint.fromDouble(950.0), 0L);
        engine.registerOrder(order);

        // Bid at 960 (above stop) → should not trigger
        engine.onMarketDataUpdate(1, FixedPoint.fromDouble(960.0), FixedPoint.fromDouble(970.0));
        assertTrue(triggers.isEmpty());
    }

    @Test
    void testStopLimitBuy() {
        OmsOrder order = createStopOrder(3L, OrderSide.BUY, OmsOrderType.STOP_LIMIT,
                FixedPoint.fromDouble(1050.0), FixedPoint.fromDouble(1060.0));
        engine.registerOrder(order);

        // Ask reaches 1050 → trigger
        engine.onMarketDataUpdate(1, FixedPoint.fromDouble(1040.0), FixedPoint.fromDouble(1050.0));

        assertEquals(1, triggers.size());
        assertEquals(OmsOrderType.LIMIT, triggers.get(0).childType());
        assertEquals(FixedPoint.fromDouble(1060.0), triggers.get(0).childPrice());
    }

    @Test
    void testTrailingStopSell() {
        OmsOrder order = createTrailingOrder(4L, OrderSide.SELL, FixedPoint.fromDouble(50.0));

        // Initialize market at bid=1000
        engine.onMarketDataUpdate(1, FixedPoint.fromDouble(1000.0), FixedPoint.fromDouble(1010.0));
        engine.registerOrder(order);

        // Bid rises to 1100 → arm price updates
        engine.onMarketDataUpdate(1, FixedPoint.fromDouble(1100.0), FixedPoint.fromDouble(1110.0));
        assertTrue(triggers.isEmpty());

        // Bid drops to 1050 (1100 - 1050 = 50 = delta) → should trigger
        engine.onMarketDataUpdate(1, FixedPoint.fromDouble(1050.0), FixedPoint.fromDouble(1060.0));
        assertEquals(1, triggers.size());
        assertEquals(OmsOrderType.MARKET, triggers.get(0).childType());
    }

    @Test
    void testIcebergSliceFilled() {
        OmsOrder order = new OmsOrder();
        order.setOmsOrderId(5L);
        order.setUserId(100L);
        order.setMarketId(1);
        order.setSide(OrderSide.BUY);
        order.setOrderType(OmsOrderType.ICEBERG);
        order.setPrice(FixedPoint.fromDouble(1000.0));
        order.setQuantity(FixedPoint.fromDouble(10.0));
        order.setRemainingQty(FixedPoint.fromDouble(10.0));
        order.setDisplayQuantity(FixedPoint.fromDouble(2.0));
        order.setHiddenQuantity(FixedPoint.fromDouble(10.0));
        order.setStatus(OmsOrderStatus.PENDING_TRIGGER);

        engine.registerOrder(order);
        engine.onIcebergSliceFilled(5L);

        assertEquals(1, icebergEvents.size());
        assertEquals(FixedPoint.fromDouble(2.0), icebergEvents.get(0).nextSliceQty());
    }

    @Test
    void testRemoveOrder() {
        OmsOrder order = createStopOrder(6L, OrderSide.SELL, OmsOrderType.STOP_LOSS,
                FixedPoint.fromDouble(950.0), 0L);
        engine.registerOrder(order);
        assertEquals(1, engine.getActiveStopCount());

        engine.removeOrder(order);
        assertEquals(0, engine.getActiveStopCount());

        // Should not trigger after removal
        engine.onMarketDataUpdate(1, FixedPoint.fromDouble(900.0), FixedPoint.fromDouble(910.0));
        assertTrue(triggers.isEmpty());
    }

    private OmsOrder createStopOrder(long id, OrderSide side, OmsOrderType type, long stopPrice, long price) {
        OmsOrder order = new OmsOrder();
        order.setOmsOrderId(id);
        order.setUserId(100L);
        order.setMarketId(1);
        order.setSide(side);
        order.setOrderType(type);
        order.setPrice(price);
        order.setQuantity(FixedPoint.fromDouble(1.0));
        order.setRemainingQty(FixedPoint.fromDouble(1.0));
        order.setStopPrice(stopPrice);
        order.setStatus(OmsOrderStatus.PENDING_TRIGGER);
        return order;
    }

    private OmsOrder createTrailingOrder(long id, OrderSide side, long delta) {
        OmsOrder order = new OmsOrder();
        order.setOmsOrderId(id);
        order.setUserId(100L);
        order.setMarketId(1);
        order.setSide(side);
        order.setOrderType(OmsOrderType.TRAILING_STOP);
        order.setTrailingDelta(delta);
        order.setQuantity(FixedPoint.fromDouble(1.0));
        order.setRemainingQty(FixedPoint.fromDouble(1.0));
        order.setStatus(OmsOrderStatus.PENDING_TRIGGER);
        return order;
    }
}
