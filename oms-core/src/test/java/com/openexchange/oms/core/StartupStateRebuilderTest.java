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
        lifecycleA.applyFill(101, 30);
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
                persistedCopy(buy), persistedCopy(rest), persistedCopy(stop), persistedCopy(limbo));
        List<PositionAggregate> aggregates = List.of(
                new PositionAggregate(1, MARKET, 30),
                new PositionAggregate(2, MARKET, -30));

        OrderLifecycleManager lifecycleB = new OrderLifecycleManager();
        SyntheticOrderEngine syntheticB = new SyntheticOrderEngine();
        RiskEngine riskB = newRiskEngine();

        StartupStateRebuilder.Result result = StartupStateRebuilder.rebuild(
                persistedOpenOrders, aggregates, lifecycleB, syntheticB, riskB);

        assertEquals(3, result.ordersRestored());
        assertEquals(1, result.ordersSkippedPreCluster());
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
}
