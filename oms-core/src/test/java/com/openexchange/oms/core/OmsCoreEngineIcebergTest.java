// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.core;

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

/**
 * Multi-slice ICEBERG refill end-to-end at the core-engine level (oms#86).
 * <p>
 * The trade stream drives per-slice completion (taker OR maker side); each
 * completed slice submits the next through the one submitIcebergSlice path;
 * the cluster's per-slice FILLED statuses never terminalize the parent; the
 * parent goes FILLED exactly when cumulative fills reach the total.
 */
class OmsCoreEngineIcebergTest {

    private static final int MARKET = 1;
    private static final long TOTAL = 10_00000000L;   // 10.0
    private static final long DISPLAY = 4_00000000L;  // 4.0 → slices 4 + 4 + 2

    private OrderLifecycleManager lifecycle;
    private SyntheticOrderEngine synthetic;
    private OmsCoreEngine engine;
    private final List<long[]> sliceSubmissions = new ArrayList<>(); // [omsOrderId, sliceQty]
    private long nextTradeId = 1;
    private long nextClusterOrderId = 900;

    @BeforeEach
    void setUp() {
        lifecycle = new OrderLifecycleManager();
        synthetic = new SyntheticOrderEngine();
        engine = new OmsCoreEngine(lifecycle, synthetic);
        engine.setClusterSubmitHandler(new OmsCoreEngine.ClusterSubmitHandler() {
            @Override
            public void submitTriggeredOrder(OmsOrder parentOrder, OmsOrderType childType, long childPrice) { }

            @Override
            public boolean submitIcebergSlice(OmsOrder icebergOrder, long sliceQuantity) {
                sliceSubmissions.add(new long[]{icebergOrder.getOmsOrderId(), sliceQuantity});
                return true;
            }

            @Override
            public void submitCancel(long clusterOrderId, long userId, int marketId) { }

            @Override
            public void submitOpenOrdersSnapshotRequest(long requestId) { }
        });
    }

    private OmsOrder icebergResting(long omsOrderId) {
        OmsOrder order = new OmsOrder();
        order.setOmsOrderId(omsOrderId);
        order.setUserId(100L);
        order.setMarketId(MARKET);
        order.setSide(OrderSide.SELL); // resting maker by default
        order.setOrderType(OmsOrderType.ICEBERG);
        order.setTimeInForce(TimeInForce.GTC);
        order.setPrice(100_00000000L);
        order.setQuantity(TOTAL);
        order.setRemainingQty(TOTAL);
        order.setDisplayQuantity(DISPLAY);
        order.setHiddenQuantity(TOTAL);
        lifecycle.registerOrder(order);
        lifecycle.onRiskPassed(omsOrderId);
        lifecycle.onHoldPlaced(omsOrderId);
        synthetic.registerOrder(order);
        // First display slice, through the same path production uses (arms the tracker).
        engine.submitIcebergSlice(order, Math.min(DISPLAY, TOTAL));
        // Slice rests: cluster acks with the slice's clusterOrderId.
        lifecycle.onClusterOrderStatus(omsOrderId, nextClusterOrderId++, 0, DISPLAY, 0);
        return order;
    }

    /** A trade filling {@code qty} of the iceberg as the MAKER (resting side). */
    private void makerFill(long omsOrderId, long qty) {
        engine.onTradeExecution(MARKET, nextTradeId++, /*takerOrderId*/ 1L, /*makerOrderId*/ 2L,
                /*takerUserId*/ 200L, /*makerUserId*/ 100L, 100_00000000L, qty,
                /*takerIsBuy*/ true, /*takerOmsOrderId*/ 0L, /*makerOmsOrderId*/ omsOrderId);
    }

    @Test
    void multiSliceRefillDrivenByMakerFills() {
        OmsOrder order = icebergResting(1L);
        assertEquals(1, sliceSubmissions.size(), "first slice submitted at creation");
        assertEquals(DISPLAY, sliceSubmissions.get(0)[1]);

        // Two partial fills complete slice 1 (2.5 + 1.5 = 4.0): refill fires ONCE, 4.0 again.
        makerFill(1L, 2_50000000L);
        assertEquals(1, sliceSubmissions.size(), "mid-slice fill must not refill");
        makerFill(1L, 1_50000000L);
        assertEquals(2, sliceSubmissions.size(), "slice 1 complete → slice 2 submitted");
        assertEquals(DISPLAY, sliceSubmissions.get(1)[1]);
        assertEquals(OmsOrderStatus.PARTIALLY_FILLED, order.getStatus());

        // The cluster's FILLED for slice 1 arrives (slice cid, parent only 4/10 filled):
        // bookkeeping only — parent stays active, cid cleared for the next slice's ack.
        lifecycle.onClusterOrderStatus(1L, 900L, 2, 0, DISPLAY);
        assertNotNull(lifecycle.getOrder(1L), "slice FILLED must not terminalize the parent");
        assertEquals(OmsOrderStatus.PARTIALLY_FILLED, order.getStatus());
        assertEquals(0, order.getClusterOrderId());

        // Slice 2 acks under a new cid and fills fully → final short slice (2.0) submitted.
        lifecycle.onClusterOrderStatus(1L, nextClusterOrderId++, 0, DISPLAY, 0);
        makerFill(1L, DISPLAY);
        assertEquals(3, sliceSubmissions.size(), "slice 2 complete → final slice submitted");
        assertEquals(TOTAL - 2 * DISPLAY, sliceSubmissions.get(2)[1], "final slice is the short remainder");

        // Final slice acks and fills: parent FILLED with exact cumulative quantity.
        lifecycle.onClusterOrderStatus(1L, nextClusterOrderId++, 0, TOTAL - 2 * DISPLAY, 0);
        makerFill(1L, TOTAL - 2 * DISPLAY);
        assertEquals(OmsOrderStatus.FILLED, order.getStatus());
        assertEquals(TOTAL, order.getFilledQty());
        assertNull(lifecycle.getOrder(1L), "parent removed from active map on true FILLED");
        assertEquals(3, sliceSubmissions.size(), "no refill after exhaustion");
        assertEquals(0, synthetic.getActiveIcebergCount(), "synthetic tracking self-cleaned");
    }

    @Test
    void takerSideSliceFillsAlsoRefill() {
        OmsOrder order = icebergResting(2L);
        // Same trade shape but the iceberg is the TAKER (e.g. a slice that crossed on entry).
        engine.onTradeExecution(MARKET, nextTradeId++, 1L, 2L, 100L, 300L, 100_00000000L,
                DISPLAY, /*takerIsBuy*/ false, /*takerOmsOrderId*/ 2L, /*makerOmsOrderId*/ 0L);
        assertEquals(2, sliceSubmissions.size(), "taker-side slice completion must refill too");
        assertEquals(OmsOrderStatus.PARTIALLY_FILLED, order.getStatus());
    }

    @Test
    void sliceFilledStatusBeforeTradesIsBookkeepingOnly() {
        // Status stream can outrun the trade stream: FILLED(slice cid) with parent fills
        // still at 0 must not terminalize, not touch filledQty, and free the cid.
        OmsOrder order = icebergResting(3L);
        long sliceCid = order.getClusterOrderId();
        lifecycle.onClusterOrderStatus(3L, sliceCid, 2, 0, DISPLAY);
        assertNotNull(lifecycle.getOrder(3L));
        assertEquals(0, order.getClusterOrderId());
        assertNull(lifecycle.getByClusterOrderId(sliceCid));
        // The trades then land and drive the refill exactly once.
        makerFill(3L, DISPLAY);
        assertEquals(2, sliceSubmissions.size());
    }

    @Test
    void nextSliceNewRelinksWhenPriorSliceFilledWasCoalescedAway() {
        OmsOrder order = icebergResting(4L);
        long slice1Cid = order.getClusterOrderId();
        makerFill(4L, DISPLAY); // slice 1 done, slice 2 submitted
        // Slice 1's FILLED status was coalesced away; slice 2's NEW arrives while the
        // parent still points at slice 1. The iceberg re-link arm adopts the new cid.
        long slice2Cid = nextClusterOrderId++;
        lifecycle.onClusterOrderStatus(4L, slice2Cid, 0, DISPLAY, 0);
        assertEquals(slice2Cid, order.getClusterOrderId());
        assertSame(order, lifecycle.getByClusterOrderId(slice2Cid));
        assertNull(lifecycle.getByClusterOrderId(slice1Cid));
        // A late redelivered FILLED for slice 1 is a stale-leg echo: ignored.
        lifecycle.onClusterOrderStatus(4L, slice1Cid, 2, 0, DISPLAY);
        assertNotNull(lifecycle.getOrder(4L));
        assertEquals(OmsOrderStatus.PARTIALLY_FILLED, order.getStatus());
    }
}
