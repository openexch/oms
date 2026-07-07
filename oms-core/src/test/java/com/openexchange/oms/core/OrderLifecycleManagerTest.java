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

    // ==================== Cancel-and-replace (oms#67) ====================

    private static class RecordingHooks implements OrderLifecycleManager.ReplaceHooks {
        int resolved;
        int aborted;
        long lastAbortedDelta = -1;

        @Override
        public void onReplaceResolved(OmsOrder order) {
            resolved++;
            order.setHoldAmount(order.getPendingHoldTarget());
        }

        @Override
        public void onReplaceAborted(OmsOrder order) {
            aborted++;
            lastAbortedDelta = order.getPendingHoldDelta();
        }
    }

    /** Register an order and walk it to NEW on cluster leg {@code cid}. */
    private OmsOrder restingOrder(long id, long cid) {
        OmsOrder order = createOrder(id);
        lcm.registerOrder(order);
        lcm.onRiskPassed(id);
        lcm.onHoldPlaced(id);
        lcm.onClusterOrderStatus(id, cid, 0, order.getQuantity(), 0);
        assertEquals(OmsOrderStatus.NEW, order.getStatus());
        return order;
    }

    @Test
    void testReplaceCancelledFirstThenNew() {
        RecordingHooks hooks = new RecordingHooks();
        lcm.setReplaceHooks(hooks);
        OmsOrder order = restingOrder(10L, 500L);
        order.setHoldAmount(100L);

        assertTrue(lcm.onReplaceSubmitted(10L, 120_000_000L, 100_000_000L, 0, 120L));
        // Engine ordering A: CANCELLED(old) first — must NOT terminalize.
        lcm.onClusterOrderStatus(10L, 500L, 3, 0, 0);
        assertEquals(OmsOrderStatus.NEW, order.getStatus());
        assertNotNull(lcm.getOrder(10L));
        assertEquals(0, order.getClusterOrderId());
        assertNull(lcm.getByClusterOrderId(500L));
        assertFalse(stateTransitions.contains(OmsOrderStatus.CANCELLED));

        // NEW(new leg) resolves: re-linked, price/qty applied, hooks fired.
        lcm.onClusterOrderStatus(10L, 501L, 0, 100_000_000L, 0);
        assertEquals(OmsOrderStatus.NEW, order.getStatus());
        assertEquals(501L, order.getClusterOrderId());
        assertSame(order, lcm.getByClusterOrderId(501L));
        assertEquals(120_000_000L, order.getPrice());
        assertEquals(120L, order.getHoldAmount());
        assertFalse(order.isReplacePending());
        assertEquals(1, hooks.resolved);
        assertEquals(0, hooks.aborted);
    }

    @Test
    void testReplaceNewFirstThenStaleCancelledIgnored() {
        RecordingHooks hooks = new RecordingHooks();
        lcm.setReplaceHooks(hooks);
        OmsOrder order = restingOrder(11L, 600L);

        assertTrue(lcm.onReplaceSubmitted(11L, 90_000_000L, 100_000_000L, 0, 90L));
        // Engine ordering B: the new leg's NEW arrives first.
        lcm.onClusterOrderStatus(11L, 601L, 0, 100_000_000L, 0);
        assertEquals(601L, order.getClusterOrderId());
        assertEquals(90_000_000L, order.getPrice());
        assertFalse(order.isReplacePending());
        assertEquals(1, hooks.resolved);

        // The old leg's CANCELLED lands afterwards: stale-leg guard must swallow it.
        lcm.onClusterOrderStatus(11L, 600L, 3, 0, 0);
        assertEquals(OmsOrderStatus.NEW, order.getStatus());
        assertNotNull(lcm.getOrder(11L));
        assertEquals(601L, order.getClusterOrderId());
        assertFalse(stateTransitions.contains(OmsOrderStatus.CANCELLED));
    }

    @Test
    void testSecondAmendRejectedWhilePending() {
        lcm.setReplaceHooks(new RecordingHooks());
        OmsOrder order = restingOrder(12L, 700L);
        assertTrue(lcm.onReplaceSubmitted(12L, 110_000_000L, 100_000_000L, 0, 110L));
        assertFalse(lcm.onReplaceSubmitted(12L, 115_000_000L, 100_000_000L, 0, 115L));
        // First amend's values stay pending.
        assertEquals(110_000_000L, order.getPendingPrice());
    }

    @Test
    void testAmendRejectedByEngineOldLegIntact() {
        RecordingHooks hooks = new RecordingHooks();
        lcm.setReplaceHooks(hooks);
        OmsOrder order = restingOrder(13L, 800L);
        order.setHoldAmount(100L);

        assertTrue(lcm.onReplaceSubmitted(13L, 130_000_000L, 100_000_000L, 0, 130L));
        order.setPendingHoldDelta(30L); // as the service does after a successful delta hold

        // Engine refuses the amend: REJECTED names the OLD leg, old order intact.
        lcm.onClusterOrderStatus(13L, 800L, 4, 0, 0);
        assertEquals(OmsOrderStatus.NEW, order.getStatus());
        assertNotNull(lcm.getOrder(13L));
        assertEquals(800L, order.getClusterOrderId());
        assertEquals(100_000_000L, order.getPrice());
        assertEquals(100L, order.getHoldAmount());
        assertFalse(order.isReplacePending());
        assertEquals(0, hooks.resolved);
        assertEquals(1, hooks.aborted);
        assertEquals(30L, hooks.lastAbortedDelta);
        assertFalse(stateTransitions.contains(OmsOrderStatus.REJECTED));
    }

    @Test
    void testNewLegCouldNotRestTerminalizes() {
        RecordingHooks hooks = new RecordingHooks();
        lcm.setReplaceHooks(hooks);
        OmsOrder order = restingOrder(14L, 900L);

        assertTrue(lcm.onReplaceSubmitted(14L, 140_000_000L, 100_000_000L, 0, 140L));
        // Old leg cancelled...
        lcm.onClusterOrderStatus(14L, 900L, 3, 0, 0);
        assertNotNull(lcm.getOrder(14L));
        // ...but the new leg could not rest: REJECTED names the NEW leg — a real outcome,
        // resolve-then-terminalize (holds now sized to the amended order, released via listener).
        lcm.onClusterOrderStatus(14L, 901L, 4, 0, 0);
        assertEquals(OmsOrderStatus.REJECTED, order.getStatus());
        assertNull(lcm.getOrder(14L));
        assertEquals(1, hooks.resolved);
    }

    @Test
    void testOldLegFilledWhilePendingAborts() {
        RecordingHooks hooks = new RecordingHooks();
        lcm.setReplaceHooks(hooks);
        OmsOrder order = restingOrder(15L, 1000L);
        order.setHoldAmount(100L);

        assertTrue(lcm.onReplaceSubmitted(15L, 150_000_000L, 100_000_000L, 0, 150L));
        order.setPendingHoldDelta(50L);

        // A fill races the amend's cancel: the old leg goes FILLED. The replace must abort
        // (releasing the delta) and the FILLED terminal proceed normally.
        lcm.onClusterOrderStatus(15L, 1000L, 2, 0, order.getQuantity());
        assertEquals(OmsOrderStatus.FILLED, order.getStatus());
        assertNull(lcm.getOrder(15L));
        assertEquals(100_000_000L, order.getPrice()); // amend never applied
        assertEquals(1, hooks.aborted);
        assertEquals(50L, hooks.lastAbortedDelta);
        assertEquals(0, hooks.resolved);
    }

    @Test
    void testNewLegInstantFillResolvesThenTerminalizes() {
        RecordingHooks hooks = new RecordingHooks();
        lcm.setReplaceHooks(hooks);
        OmsOrder order = restingOrder(16L, 1100L);

        assertTrue(lcm.onReplaceSubmitted(16L, 160_000_000L, 100_000_000L, 0, 160L));
        lcm.onClusterOrderStatus(16L, 1100L, 3, 0, 0); // old leg cancelled
        // New leg crossed and filled instantly: resolve first, then the FILLED terminal.
        lcm.onClusterOrderStatus(16L, 1101L, 2, 0, 100_000_000L);
        assertEquals(OmsOrderStatus.FILLED, order.getStatus());
        assertEquals(100_000_000L, order.getFilledQty());
        assertEquals(160_000_000L, order.getPrice());
        assertEquals(1, hooks.resolved);
        assertNull(lcm.getOrder(16L));
    }

    @Test
    void testReplaceRequiresLiveClusterOrder() {
        lcm.setReplaceHooks(new RecordingHooks());
        OmsOrder order = createOrder(17L);
        lcm.registerOrder(order);
        // Not yet on the cluster (cid == 0) → no replace.
        assertFalse(lcm.onReplaceSubmitted(17L, 120_000_000L, 100_000_000L, 0, 120L));
        assertFalse(order.isReplacePending());
    }
}
