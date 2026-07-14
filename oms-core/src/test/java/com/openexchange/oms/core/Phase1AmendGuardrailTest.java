// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.core;

import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.common.enums.OmsOrderType;
import com.openexchange.oms.common.enums.OrderSide;
import com.openexchange.oms.common.enums.TimeInForce;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase-1 money-state GUARDRAIL — amend/replace hold lifecycle (OMS-1, OMS-2).
 * <p>
 * These reproduce confirmed fund-integrity defects deterministically and are the
 * regression gate for the fixes. They FAIL against the pre-fix code (that is the
 * point) and must pass once the ordered/versioned hold-state machine lands.
 * <p>
 * The framing (per the money-state ordering principle): the cluster log is a
 * total order over every action, so each scenario asserts the order-correct
 * INVARIANT — exactly one amend wins (linearizable), and a fill that terminalizes
 * an order releases any pending amend hold.
 */
class Phase1AmendGuardrailTest {

    /** Walk a fresh limit BUY to NEW on cluster leg {@code cid} so it is amendable. */
    private static OmsOrder restingBuy(OrderLifecycleManager lcm, long omsOrderId, long cid) {
        OmsOrder order = new OmsOrder();
        order.setOmsOrderId(omsOrderId);
        order.setUserId(100L);
        order.setMarketId(1);
        order.setSide(OrderSide.BUY);
        order.setOrderType(OmsOrderType.LIMIT);
        order.setTimeInForce(TimeInForce.GTC);
        order.setPrice(100_000_000L);
        order.setQuantity(100_000_000L);
        order.setRemainingQty(100_000_000L);
        lcm.registerOrder(order);
        lcm.onRiskPassed(omsOrderId);
        lcm.onHoldPlaced(omsOrderId);
        lcm.onClusterOrderStatus(omsOrderId, cid, 0, order.getQuantity(), 0);
        assertEquals(OmsOrderStatus.NEW, order.getStatus());
        return order;
    }

    private static final class RecordingHooks implements OrderLifecycleManager.ReplaceHooks {
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

    /**
     * OMS-1 — concurrent-amend double-hold. {@code onReplaceSubmitted}'s guard is a
     * compound check-then-set: it reads {@code isReplacePending()} (238) and only later
     * writes the replace marker (248), with no CAS or per-order lock. On two Netty I/O
     * threads (OmsOrderServiceImpl.updateOrder) two amends for one order can both pass the
     * guard, both return true, and both place an incremental hold — permanently
     * double-locking funds. The ordered fix makes the marker a single-winner transition.
     * <p>
     * Invariant (linearizability): across many concurrent amend pairs on one order, AT
     * MOST ONE onReplaceSubmitted may win. Two winners == the double-hold bug.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void oms1_concurrentAmendWinsExactlyOnce() throws Exception {
        final int rounds = 20_000;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int r = 0; r < rounds; r++) {
                OrderLifecycleManager lcm = new OrderLifecycleManager();
                lcm.setReplaceHooks(new RecordingHooks());
                final OmsOrder order = restingBuy(lcm, 1L, 500L);

                final CyclicBarrier gate = new CyclicBarrier(2);
                Callable<Boolean> amend = () -> {
                    gate.await();
                    // Two distinct amend targets so a winner is unambiguous.
                    return lcm.onReplaceSubmitted(order.getOmsOrderId(),
                            110_000_000L, 100_000_000L, 0, 110L);
                };
                Future<Boolean> f1 = pool.submit(amend);
                Future<Boolean> f2 = pool.submit(amend);
                int winners = (f1.get() ? 1 : 0) + (f2.get() ? 1 : 0);
                assertEquals(1, winners,
                        "exactly one concurrent amend may win the replace marker (round " + r + ")");
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * OMS-2 — a fill that terminalizes a replace-pending order leaks the amend hold.
     * Fills are applied authoritatively through {@code applyFill} (the TradeExecution
     * stream). When a fill completes an order mid-amend, applyFill drives it straight to
     * FILLED + removeOrder WITHOUT aborting the pending replace — so the {@code
     * pendingHoldDelta} incremental hold is never released. The {@code onClusterOrderStatus}
     * fill path already aborts (status==2 → abortReplace); applyFill must match it.
     * <p>
     * Invariant: a fill terminalizing a replace-pending order fires onReplaceAborted so the
     * amend delta is released.
     */
    @Test
    void oms2_fillTerminalizingMidAmendReleasesTheDelta() {
        OrderLifecycleManager lcm = new OrderLifecycleManager();
        RecordingHooks hooks = new RecordingHooks();
        lcm.setReplaceHooks(hooks);
        OmsOrder order = restingBuy(lcm, 20L, 1000L);
        order.setHoldAmount(100L);

        // A grown amend is in flight: +50 was locked at submit (pendingHoldDelta > 0).
        assertTrue(lcm.onReplaceSubmitted(20L, 150_000_000L, 100_000_000L, 0, 150L));
        order.setPendingHoldDelta(50L);

        // The old leg fully fills via the trade stream before the amend resolves.
        lcm.applyFill(20L, 1000L, order.getQuantity());

        assertEquals(OmsOrderStatus.FILLED, order.getStatus());
        assertEquals(1, hooks.aborted,
                "a fill terminalizing a replace-pending order must abort the replace so the amend delta is released");
        assertEquals(50L, hooks.lastAbortedDelta);
        assertEquals(0, hooks.resolved);
    }
}
