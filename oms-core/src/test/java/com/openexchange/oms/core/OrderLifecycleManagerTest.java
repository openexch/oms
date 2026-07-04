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

class OrderLifecycleManagerTest {

    private OrderLifecycleManager lcm;
    private List<OmsOrderStatus> stateTransitions;

    @BeforeEach
    void setUp() {
        lcm = new OrderLifecycleManager();
        stateTransitions = new ArrayList<>();
        lcm.setStateListener((order, oldStatus, newStatus) -> stateTransitions.add(newStatus));
    }

    @Test
    void testHappyPath() {
        OmsOrder order = createOrder(1L);
        lcm.registerOrder(order);
        assertEquals(OmsOrderStatus.PENDING_RISK, order.getStatus());

        lcm.onRiskPassed(order.getOmsOrderId());
        assertEquals(OmsOrderStatus.PENDING_HOLD, order.getStatus());

        lcm.onHoldPlaced(order.getOmsOrderId());
        assertEquals(OmsOrderStatus.PENDING_NEW, order.getStatus());

        // Cluster accepts → NEW
        lcm.onClusterOrderStatus(order.getOmsOrderId(), 500L, 0, 0, 0);
        assertEquals(OmsOrderStatus.NEW, order.getStatus());

        // Partially filled
        lcm.onClusterOrderStatus(order.getOmsOrderId(), 500L, 1, 50_000_000L, 50_000_000L);
        assertEquals(OmsOrderStatus.PARTIALLY_FILLED, order.getStatus());

        // Fully filled
        lcm.onClusterOrderStatus(order.getOmsOrderId(), 500L, 2, 0L, 100_000_000L);
        assertEquals(OmsOrderStatus.FILLED, order.getStatus());

        // Order should be removed from active
        assertNull(lcm.getOrder(order.getOmsOrderId()));

        // Verify all transitions fired
        assertTrue(stateTransitions.contains(OmsOrderStatus.PENDING_HOLD));
        assertTrue(stateTransitions.contains(OmsOrderStatus.PENDING_NEW));
        assertTrue(stateTransitions.contains(OmsOrderStatus.NEW));
        assertTrue(stateTransitions.contains(OmsOrderStatus.PARTIALLY_FILLED));
        assertTrue(stateTransitions.contains(OmsOrderStatus.FILLED));
    }

    @Test
    void testRiskRejection() {
        OmsOrder order = createOrder(2L);
        lcm.registerOrder(order);

        lcm.onRiskRejected(order.getOmsOrderId(), "RATE_LIMIT_EXCEEDED");
        assertEquals(OmsOrderStatus.REJECTED, order.getStatus());
        assertEquals("RATE_LIMIT_EXCEEDED", order.getRejectReason());

        // Removed from active
        assertNull(lcm.getOrder(order.getOmsOrderId()));
    }

    @Test
    void testHoldFailed() {
        OmsOrder order = createOrder(3L);
        lcm.registerOrder(order);
        lcm.onRiskPassed(order.getOmsOrderId());

        lcm.onHoldFailed(order.getOmsOrderId(), "Insufficient balance");
        assertEquals(OmsOrderStatus.REJECTED, order.getStatus());
        assertNull(lcm.getOrder(order.getOmsOrderId()));
    }

    @Test
    void testCancelRequest() {
        OmsOrder order = createOrder(4L);
        lcm.registerOrder(order);
        lcm.onRiskPassed(order.getOmsOrderId());
        lcm.onHoldPlaced(order.getOmsOrderId());

        OmsOrder cancelledOrder = lcm.onCancelRequested(order.getOmsOrderId());
        assertNotNull(cancelledOrder);
        assertEquals(order.getOmsOrderId(), cancelledOrder.getOmsOrderId());
    }

    @Test
    void testCancelTerminalReturnsNull() {
        OmsOrder order = createOrder(5L);
        lcm.registerOrder(order);
        lcm.onRiskRejected(order.getOmsOrderId(), "test");

        OmsOrder result = lcm.onCancelRequested(order.getOmsOrderId());
        assertNull(result); // Already terminal + removed
    }

    @Test
    void testSyntheticOrderPendingTrigger() {
        OmsOrder order = createOrder(6L);
        order.setOrderType(OmsOrderType.STOP_LOSS);
        lcm.registerOrder(order);
        lcm.onRiskPassed(order.getOmsOrderId());
        lcm.onHoldPlaced(order.getOmsOrderId());

        lcm.onPendingTrigger(order.getOmsOrderId());
        assertEquals(OmsOrderStatus.PENDING_TRIGGER, order.getStatus());

        // Still active
        assertNotNull(lcm.getOrder(order.getOmsOrderId()));
    }

    @Test
    void testExpiry() {
        OmsOrder order = createOrder(7L);
        lcm.registerOrder(order);
        lcm.onRiskPassed(order.getOmsOrderId());
        lcm.onHoldPlaced(order.getOmsOrderId());

        OmsOrder expired = lcm.onExpired(order.getOmsOrderId());
        assertNotNull(expired);
        assertEquals(OmsOrderStatus.EXPIRED, expired.getStatus());
        assertNull(lcm.getOrder(order.getOmsOrderId()));
    }

    @Test
    void testActiveOrderCount() {
        assertEquals(0, lcm.getActiveOrderCount());
        OmsOrder o1 = createOrder(10L);
        OmsOrder o2 = createOrder(20L);
        lcm.registerOrder(o1);
        lcm.registerOrder(o2);
        assertEquals(2, lcm.getActiveOrderCount());

        lcm.onRiskRejected(o1.getOmsOrderId(), "test");
        assertEquals(1, lcm.getActiveOrderCount());
    }

    // ==================== Bug #9: filledQty from TradeExecution ====================

    @Test
    void testApplyFillAccumulatesAndDrivesStatus() {
        OmsOrder order = createOrder(30L);   // quantity = 100_000_000
        lcm.registerOrder(order);
        lcm.onRiskPassed(order.getOmsOrderId());
        lcm.onHoldPlaced(order.getOmsOrderId());
        lcm.onClusterOrderStatus(order.getOmsOrderId(), 530L, 0, 0, 0); // NEW

        OmsOrder afterPartial = lcm.applyFill(order.getOmsOrderId(), 30_000_000L);
        assertSame(order, afterPartial);
        assertEquals(OmsOrderStatus.PARTIALLY_FILLED, order.getStatus());
        assertEquals(30_000_000L, order.getFilledQty());
        assertEquals(70_000_000L, order.getRemainingQty());

        OmsOrder afterFull = lcm.applyFill(order.getOmsOrderId(), 70_000_000L);
        assertSame(order, afterFull);                 // returned even though removed
        assertEquals(OmsOrderStatus.FILLED, order.getStatus());
        assertEquals(100_000_000L, order.getFilledQty());
        assertEquals(0L, order.getRemainingQty());
        assertNull(lcm.getOrder(order.getOmsOrderId())); // removed on FILLED
    }

    @Test
    void testApplyFillOnTerminalOrUnknownIsNoOp() {
        assertNull(lcm.applyFill(999L, 10_000_000L)); // unknown
        OmsOrder order = createOrder(31L);
        lcm.registerOrder(order);
        lcm.onRiskRejected(order.getOmsOrderId(), "x"); // terminal + removed
        assertNull(lcm.applyFill(order.getOmsOrderId(), 10_000_000L));
    }

    @Test
    void testMonotonicGuardRejectsRegressingOrderStatus() {
        OmsOrder order = createOrder(32L);
        lcm.registerOrder(order);
        lcm.onRiskPassed(order.getOmsOrderId());
        lcm.onHoldPlaced(order.getOmsOrderId());
        lcm.onClusterOrderStatus(order.getOmsOrderId(), 532L, 0, 0, 0); // NEW

        lcm.applyFill(order.getOmsOrderId(), 60_000_000L);
        assertEquals(60_000_000L, order.getFilledQty());

        // A stale/coalesced OrderStatus reporting only 20M filled must NOT regress filledQty.
        lcm.onClusterOrderStatus(order.getOmsOrderId(), 532L, 1, 80_000_000L, 20_000_000L);
        assertEquals(60_000_000L, order.getFilledQty());
        assertEquals(40_000_000L, order.getRemainingQty());
        assertEquals(OmsOrderStatus.PARTIALLY_FILLED, order.getStatus());
    }

    @Test
    void testFillBeforeNewDoesNotRegressStatus() {
        OmsOrder order = createOrder(33L);
        lcm.registerOrder(order);
        lcm.onRiskPassed(order.getOmsOrderId());
        lcm.onHoldPlaced(order.getOmsOrderId());

        // Trade arrives before the cluster NEW status (reordered egress)
        lcm.applyFill(order.getOmsOrderId(), 40_000_000L);
        assertEquals(OmsOrderStatus.PARTIALLY_FILLED, order.getStatus());

        // A late NEW status must not regress the order back to NEW
        lcm.onClusterOrderStatus(order.getOmsOrderId(), 533L, 0, 60_000_000L, 0);
        assertEquals(OmsOrderStatus.PARTIALLY_FILLED, order.getStatus());
        assertEquals(40_000_000L, order.getFilledQty()); // monotonic guard preserves it
    }

    private OmsOrder createOrder(long id) {
        OmsOrder order = new OmsOrder();
        order.setOmsOrderId(id);
        order.setUserId(100L);
        order.setMarketId(1);
        order.setSide(OrderSide.BUY);
        order.setOrderType(OmsOrderType.LIMIT);
        order.setTimeInForce(TimeInForce.GTC);
        order.setPrice(100_000_000L);
        order.setQuantity(100_000_000L);
        order.setRemainingQty(100_000_000L);
        return order;
    }
}
