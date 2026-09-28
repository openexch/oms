// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.core;

import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.common.enums.OmsOrderType;
import com.openexchange.oms.common.enums.OrderSide;
import com.openexchange.oms.common.enums.TimeInForce;
import com.openexchange.oms.persistence.PositionAggregate;
import com.openexchange.oms.risk.RiskEngine;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * oms#35 exit criterion: an OMS restart reproduces positions/open-orders
 * identical to pre-restart state.
 *
 * "Pre-restart" state is built through the same lifecycle/risk calls the live
 * wiring uses; the "restart" feeds Postgres-shaped copies of the open orders
 * plus the executions position aggregate into fresh components via
 * StartupStateRebuilder, and the two states are compared.
 */
class StartupStateRebuilderTest {

    private static final int MARKET = 1;

    private RiskEngine newRiskEngine() {
        com.openexchange.oms.risk.MarketDataProvider marketData = new com.openexchange.oms.risk.MarketDataProvider() {
            @Override
            public long getLastTradePrice(int marketId) { return 0; }

            @Override
            public long getBestBid(int marketId) { return 0; }

            @Override
            public long getBestAsk(int marketId) { return 0; }
        };
        return new RiskEngine(6, marketData, (userId, assetId, amount) -> true);
    }

    private OmsOrder order(long omsId, long userId, OmsOrderType type, OrderSide side,
                           long price, long qty) {
        OmsOrder o = new OmsOrder();
        o.setOmsOrderId(omsId);
        o.setStateRevision(1);
        o.setUserId(userId);
        o.setMarketId(MARKET);
        o.setOrderType(type);
        o.setSide(side);
        o.setTimeInForce(TimeInForce.GTC);
        o.setPrice(price);
        o.setQuantity(qty);
        o.setRemainingQty(qty);
        return o;
    }

    /** Simulates the Postgres round trip: copy exactly the fields mapRow restores. */
    private OmsOrder persistedCopy(OmsOrder o) {
        OmsOrder c = new OmsOrder();
        c.setOmsOrderId(o.getOmsOrderId());
        c.setStateRevision(o.getStateRevision());
        c.setClusterOrderId(o.getClusterOrderId());
        c.setClientOrderId(o.getClientOrderId());
        c.setUserId(o.getUserId());
        c.setMarketId(o.getMarketId());
        c.setSide(o.getSide());
        c.setOrderType(o.getOrderType());
        c.setTimeInForce(o.getTimeInForce());
        c.setPrice(o.getPrice());
        c.setQuantity(o.getQuantity());
        c.setFilledQty(o.getFilledQty());
        c.setRemainingQty(o.getRemainingQty());
        c.setStopPrice(o.getStopPrice());
        c.setTrailingDelta(o.getTrailingDelta());
        c.setDisplayQuantity(o.getDisplayQuantity());
        c.setStatus(o.getStatus());
        c.setHoldAmount(o.getHoldAmount());
        c.setExpiresAtMs(o.getExpiresAtMs());
        c.setCreatedAtMs(o.getCreatedAtMs());
        c.setUpdatedAtMs(o.getUpdatedAtMs());
        return c;
    }

    @Test
    void ambiguousHoldIntentCannotBeSilentlySkippedOnRestart() {
        var pending = order(999, 4, OmsOrderType.LIMIT, OrderSide.BUY, 50000, 5);
        pending.setStatus(OmsOrderStatus.PENDING_HOLD);
        var lifecycle = new OrderLifecycleManager();
        assertThrows(IllegalStateException.class, () -> StartupStateRebuilder.rebuild(
                List.of(pending), List.of(), lifecycle, new SyntheticOrderEngine(), newRiskEngine()));
        assertEquals(0, lifecycle.getActiveOrderCount());
    }

    @Test
    void rebuildReproducesPreRestartState() {
        // ---- pre-restart: live components driven through the normal call paths ----
        OrderLifecycleManager lifecycleA = new OrderLifecycleManager();
        SyntheticOrderEngine syntheticA = new SyntheticOrderEngine();
        RiskEngine riskA = newRiskEngine();

        // user 1: resting limit, partially filled 30/100 against user 2's closed order
        OmsOrder buy = order(101, 1, OmsOrderType.LIMIT, OrderSide.BUY, 50_000, 100);
        lifecycleA.registerOrder(buy);
        lifecycleA.onRiskPassed(101);
        lifecycleA.onHoldPlaced(101);
        lifecycleA.onSentToCluster(101, 501);
        riskA.onOrderOpened(1);
        lifecycleA.applyFill(101, 501, 30);
        riskA.onFill(1, MARKET, OrderSide.BUY, 30);
        riskA.onFill(2, MARKET, OrderSide.SELL, 30); // counterparty, order fully filled → not open

        // user 2: untouched resting order awaiting cluster ack
        OmsOrder rest = order(102, 2, OmsOrderType.LIMIT, OrderSide.SELL, 51_000, 40);
        lifecycleA.registerOrder(rest);
        lifecycleA.onRiskPassed(102);
        lifecycleA.onHoldPlaced(102);
        lifecycleA.onSentToCluster(102, 502);
        riskA.onOrderOpened(2);

        // user 3: stop-limit waiting for its trigger
        OmsOrder stop = order(103, 3, OmsOrderType.STOP_LIMIT, OrderSide.SELL, 48_000, 10);
        stop.setStopPrice(49_000);
        lifecycleA.registerOrder(stop);
        lifecycleA.onRiskPassed(103);
        lifecycleA.onHoldPlaced(103);
        lifecycleA.onPendingTrigger(103);
        syntheticA.registerOrder(stop);
        riskA.onOrderOpened(3);

        // user 4: restart interrupts mid-risk-check — never reached the cluster pipeline
        OmsOrder limbo = order(104, 4, OmsOrderType.LIMIT, OrderSide.BUY, 50_000, 5);
        lifecycleA.registerOrder(limbo);

        // ---- "restart": Postgres-shaped rows + executions aggregate into fresh components ----
        List<OmsOrder> persistedOpenOrders = List.of(
                persistedCopy(buy), persistedCopy(rest), persistedCopy(stop));
        List<PositionAggregate> aggregates = List.of(
                new PositionAggregate(1, MARKET, 30),
                new PositionAggregate(2, MARKET, -30));

        OrderLifecycleManager lifecycleB = new OrderLifecycleManager();
        SyntheticOrderEngine syntheticB = new SyntheticOrderEngine();
        RiskEngine riskB = newRiskEngine();

        StartupStateRebuilder.Result result = StartupStateRebuilder.rebuild(
                persistedOpenOrders, aggregates, lifecycleB, syntheticB, riskB);

        assertEquals(3, result.ordersRestored());
        assertEquals(0, result.ordersSkippedPreCluster());
        assertEquals(1, result.syntheticsRegistered());
        assertEquals(2, result.positionsRestored());

        // ---- open orders identical to pre-restart ----
        for (long id : new long[]{101, 102, 103}) {
            OmsOrder before = lifecycleA.getOrder(id);
            OmsOrder after = lifecycleB.getOrder(id);
            assertNotNull(after, "order " + id + " must be restored");
            assertEquals(before.getStatus(), after.getStatus());
            assertEquals(before.getFilledQty(), after.getFilledQty());
            assertEquals(before.getRemainingQty(), after.getRemainingQty());
            assertEquals(before.getClusterOrderId(), after.getClusterOrderId());
        }
        assertEquals(OmsOrderStatus.PARTIALLY_FILLED, lifecycleB.getOrder(101).getStatus());
        assertEquals(101, lifecycleB.getByClusterOrderId(501).getOmsOrderId());
        assertEquals(102, lifecycleB.getByClusterOrderId(502).getOmsOrderId());
        assertNull(lifecycleB.getOrder(104), "pre-cluster limbo order must not be restored");

        // ---- positions identical to pre-restart ----
        for (long userId : new long[]{1, 2, 3, 4}) {
            assertEquals(riskA.getPosition(userId, MARKET), riskB.getPosition(userId, MARKET),
                    "position of user " + userId);
        }

        // ---- open-order slots identical to pre-restart ----
        for (long userId : new long[]{1, 2, 3, 4}) {
            assertEquals(riskA.getOpenOrderCount(userId), riskB.getOpenOrderCount(userId),
                    "open-order count of user " + userId);
        }

        // ---- synthetic trigger monitoring re-armed ----
        assertEquals(syntheticA.getActiveStopCount(), syntheticB.getActiveStopCount());
    }
    @Test
    void restartPreservesTrailingExtremeBeforeFirstMarketTick() {
        var trailing = order(901, 1, OmsOrderType.TRAILING_STOP, OrderSide.SELL, 0, 10);
        trailing.setStatus(OmsOrderStatus.PENDING_TRIGGER);
        trailing.setTrailingArmPrice(1000);
        trailing.setTrailingDelta(50);
        var synthetic = new SyntheticOrderEngine();
        var triggers = new java.util.concurrent.atomic.AtomicInteger();
        synthetic.setTriggerCallback((o, t, p) -> triggers.incrementAndGet());
        StartupStateRebuilder.rebuild(List.of(trailing), List.of(), new OrderLifecycleManager(),
                synthetic, newRiskEngine());
        assertEquals(1000, trailing.getTrailingArmPrice());
        synthetic.onMarketDataUpdate(MARKET, 940, 941);
        assertEquals(1, triggers.get(), "restart must not forget the pre-crash high");
    }

    @Test
    void restartDoesNotRearmAlreadyTriggeredOrCancelledSynthetics() {
        var stop = order(902, 1, OmsOrderType.STOP_LIMIT, OrderSide.SELL, 900, 10);
        stop.setStatus(OmsOrderStatus.NEW); stop.setStopPrice(950); stop.setClusterOrderId(55);
        var trailing = order(903, 2, OmsOrderType.TRAILING_STOP, OrderSide.SELL, 0, 10);
        trailing.setStatus(OmsOrderStatus.PENDING_NEW);
        var cancelled = order(904, 3, OmsOrderType.STOP_LOSS, OrderSide.SELL, 0, 10);
        cancelled.setStatus(OmsOrderStatus.PENDING_TRIGGER); cancelled.setCancelRequested(true);
        var synthetic = new SyntheticOrderEngine();
        var result = StartupStateRebuilder.rebuild(List.of(stop, trailing, cancelled), List.of(),
                new OrderLifecycleManager(), synthetic, newRiskEngine());
        assertEquals(0, result.syntheticsRegistered());
        assertEquals(0, synthetic.getActiveStopCount());
        assertEquals(0, synthetic.getActiveTrailingCount());
    }

    @Test
    void legacyRowsWithUnknownWorkflowFieldsRequireExplicitRecovery() {
        var legacy = order(905, 1, OmsOrderType.ICEBERG, OrderSide.SELL, 900, 100);
        legacy.setStatus(OmsOrderStatus.NEW);
        legacy.setStateRevision(0);
        var lifecycle = new OrderLifecycleManager();
        assertThrows(IllegalStateException.class, () -> StartupStateRebuilder.rebuild(
                List.of(legacy), List.of(), lifecycle, new SyntheticOrderEngine(), newRiskEngine()));
        assertEquals(0, lifecycle.getActiveOrderCount());
    }

    @Test void unknownAmendHoldCannotBeRestoredAsConfirmed() {
        var amend = order(906, 1, OmsOrderType.LIMIT, OrderSide.BUY, 900, 100);
        amend.setStatus(OmsOrderStatus.NEW); amend.setClusterOrderId(44);
        amend.setReplacePendingOldClusterOrderId(44); amend.setPendingHoldRequested(100);
        amend.setPendingHoldDelta(0);
        var lifecycle = new OrderLifecycleManager();
        assertThrows(IllegalStateException.class, () -> StartupStateRebuilder.rebuild(List.of(amend),
                List.of(), lifecycle, new SyntheticOrderEngine(), newRiskEngine()));
        assertEquals(0, lifecycle.getActiveOrderCount());
    }

}
