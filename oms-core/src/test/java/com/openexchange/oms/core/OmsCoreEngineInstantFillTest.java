// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.core;

import com.openexchange.oms.common.domain.ExecutionReport;
import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.common.enums.OmsOrderType;
import com.openexchange.oms.common.enums.OrderSide;
import com.openexchange.oms.common.enums.TimeInForce;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * oms#110: an order that crosses on entry fills fully in the SAME egress batch as its accept, and
 * the flush emits TradeExecution BEFORE OrderStatus. The trade-driven path (onTradeExecution ->
 * applyFill -> persistOrderUpdate) therefore persisted the order row BEFORE any status taught it
 * its clusterOrderId, saving cluster_order_id=0; the later FILLED status could no longer find the
 * (removed) order to correct it. This drives onTradeExecution end-to-end and asserts the persisted
 * order carries the ME-assigned clusterOrderId, for both the fully-filled taker and a
 * partially-filled resting maker whose ack had not yet landed.
 */
class OmsCoreEngineInstantFillTest {

    private OrderLifecycleManager lifecycle;
    private OmsCoreEngine engine;
    /** omsOrderId -> clusterOrderId at the moment persistOrderUpdate ran (mirrors the DB upsert). */
    private Map<Long, Long> persistedCid;
    private Map<Long, OmsOrderStatus> persistedStatus;

    @BeforeEach
    void setUp() {
        lifecycle = new OrderLifecycleManager();
        engine = new OmsCoreEngine(lifecycle, new SyntheticOrderEngine());
        persistedCid = new HashMap<>();
        persistedStatus = new HashMap<>();
        engine.setSettlementHandler(
                (tradeId, buyerUserId, sellerUserId, marketId, price, quantity,
                 buyerOmsOrderId, sellerOmsOrderId) -> true); // newly applied (not a duplicate)
        engine.setPersistenceHandler(new OmsCoreEngine.PersistenceHandler() {
            @Override
            public void persistOrderUpdate(OmsOrder order) {
                persistedCid.put(order.getOmsOrderId(), order.getClusterOrderId());
                persistedStatus.put(order.getOmsOrderId(), order.getStatus());
            }

            @Override
            public void persistExecution(ExecutionReport report) { }
        });
    }

    /** Register an order and walk it to PENDING_NEW WITHOUT a clusterOrderId (ack not yet seen). */
    private OmsOrder pendingNew(long omsId, long userId, OrderSide side, long quantity) {
        OmsOrder o = new OmsOrder();
        o.setOmsOrderId(omsId);
        o.setUserId(userId);
        o.setMarketId(1);
        o.setOrderType(OmsOrderType.LIMIT);
        o.setSide(side);
        o.setTimeInForce(TimeInForce.GTC);
        o.setPrice(100_000_000L);
        o.setQuantity(quantity);
        o.setRemainingQty(quantity);
        lifecycle.registerOrder(o);
        lifecycle.onRiskPassed(omsId);
        lifecycle.onHoldPlaced(omsId);
        assertEquals(OmsOrderStatus.PENDING_NEW, o.getStatus());
        assertEquals(0L, o.getClusterOrderId());
        return o;
    }

    @Test
    void instantFillPersistsRealClusterOrderIdForBothSides() {
        // Taker crosses on entry and fully fills; resting maker's ack was lost, so it also carries
        // cid=0 and only learns it here. tradeQuantity fully fills the taker (100M) and partially
        // fills the larger maker (250M).
        OmsOrder taker = pendingNew(201L, 10L, OrderSide.BUY, 100_000_000L);
        OmsOrder maker = pendingNew(202L, 20L, OrderSide.SELL, 250_000_000L);

        final long takerMeId = 7001L; // ME-assigned cluster order ids carried on the TradeExecution
        final long makerMeId = 7002L;

        engine.onTradeExecution(
                /*marketId*/ 1, /*tradeId*/ 5001L,
                /*takerOrderId*/ takerMeId, /*makerOrderId*/ makerMeId,
                /*takerUserId*/ 10L, /*makerUserId*/ 20L,
                /*tradePrice*/ 100_000_000L, /*tradeQuantity*/ 100_000_000L,
                /*takerIsBuy*/ true,
                /*takerOmsOrderId*/ 201L, /*makerOmsOrderId*/ 202L,
                /*egressSeq*/ 0L);

        // Taker: fully filled, removed, and persisted with its REAL clusterOrderId (was the bug: 0).
        assertEquals(OmsOrderStatus.FILLED, taker.getStatus());
        assertNull(lifecycle.getOrder(201L));
        assertEquals(takerMeId, taker.getClusterOrderId());
        assertEquals(Long.valueOf(takerMeId), persistedCid.get(201L),
                "instant-filled taker must persist its ME clusterOrderId, not 0");
        assertEquals(OmsOrderStatus.FILLED, persistedStatus.get(201L));

        // Maker: partially filled, still active, cid recorded AND indexed for egress correlation.
        assertEquals(OmsOrderStatus.PARTIALLY_FILLED, maker.getStatus());
        assertEquals(makerMeId, maker.getClusterOrderId());
        assertSame(maker, lifecycle.getByClusterOrderId(makerMeId));
        assertEquals(Long.valueOf(makerMeId), persistedCid.get(202L),
                "resting maker must persist its ME clusterOrderId, not 0");
        assertEquals(OmsOrderStatus.PARTIALLY_FILLED, persistedStatus.get(202L));
    }
}
