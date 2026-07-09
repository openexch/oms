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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase-1 money-state GUARDRAIL — iceberg cancel vs. refill (OMS-6 + OMS-11).
 * <p>
 * A user cancel and a slice-fill-triggered refill race. In the cluster log they
 * are ordered; the OMS ingests them out of band. This reproduces the confirmed
 * defect deterministically and gates the fix. It FAILS against the pre-fix code.
 * <p>
 * Ordered invariant: a cancel at sequence Y suppresses any refill triggered by a
 * fill at sequence X&gt;Y. After a cancel, a completing slice-fill must NOT submit a
 * new slice (OMS-11), and the cancel must terminalize the parent rather than being
 * swallowed by the stale-leg guard once a refill re-links a fresh cid (OMS-6).
 */
class Phase1IcebergGuardrailTest {

    private static final int MARKET = 1;
    private static final long TOTAL = 10_00000000L;   // 10.0
    private static final long DISPLAY = 4_00000000L;  // 4.0

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
        order.setSide(OrderSide.SELL); // resting maker
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
        engine.submitIcebergSlice(order, Math.min(DISPLAY, TOTAL));            // first slice
        lifecycle.onClusterOrderStatus(omsOrderId, nextClusterOrderId++, 0, DISPLAY, 0); // slice rests
        return order;
    }

    /** A trade filling {@code qty} of the iceberg as the resting MAKER. */
    private void makerFill(long omsOrderId, long qty) {
        engine.onTradeExecution(MARKET, nextTradeId++, /*takerOrderId*/ 1L, /*makerOrderId*/ 2L,
                /*takerUserId*/ 200L, /*makerUserId*/ 100L, 100_00000000L, qty,
                /*takerIsBuy*/ true, /*takerOmsOrderId*/ 0L, /*makerOmsOrderId*/ omsOrderId,
                /*egressSeq*/ 0L);
    }

    @Test
    void oms6_11_cancelBeatsARacingSliceRefill() {
        OmsOrder order = icebergResting(1L);
        long workingCid = order.getClusterOrderId();
        assertEquals(1, sliceSubmissions.size(), "first slice submitted at creation");

        // The user cancels the iceberg: cancelRequested is set and a cluster cancel is sent
        // for the working slice's cid. The CANCELLED egress has not arrived yet.
        OmsOrder cancelling = lifecycle.onCancelRequested(1L);
        assertTrue(cancelling != null && order.isCancelRequested());
        int slicesBeforeRace = sliceSubmissions.size();

        // A trade completes the working display slice AFTER the cancel. On the buggy code
        // onIcebergSliceFilled ignores cancelRequested and submits a fresh slice, which
        // re-links a new cid — resurrecting an order the user asked to cancel (OMS-11).
        makerFill(1L, DISPLAY);
        assertEquals(slicesBeforeRace, sliceSubmissions.size(),
                "a cancelled iceberg must not submit a new refill slice after the cancel");

        // The CANCELLED egress for the working slice must terminalize the parent. On the
        // buggy code a refill already re-linked a new cid, so this CANCELLED is swallowed by
        // the stale-leg guard and the iceberg keeps working despite the cancel (OMS-6).
        lifecycle.onClusterOrderStatus(1L, workingCid, 3, 0, 0); // status 3 = CANCELLED
        assertNull(lifecycle.getOrder(1L),
                "the iceberg must end CANCELLED after a cancel + racing slice-fill, not keep working");
        assertEquals(0, synthetic.getActiveIcebergCount(),
                "synthetic iceberg tracking must be torn down on cancel");
    }
}
