// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.match.domain.FixedPoint;
import com.openexchange.oms.api.dto.CancelOrderResponse;
import com.openexchange.oms.api.dto.CreateOrderRequest;
import com.openexchange.oms.api.dto.CreateOrderResponse;
import com.openexchange.oms.cluster.ClusterClient;
import com.openexchange.oms.cluster.OrderSubmission;
import com.openexchange.oms.common.domain.Market;
import com.openexchange.oms.common.domain.SnowflakeIdGenerator;
import com.openexchange.oms.core.OmsCoreEngine;
import com.openexchange.oms.core.OrderLifecycleManager;
import com.openexchange.oms.core.SyntheticOrderEngine;
import com.openexchange.oms.ledger.InMemoryBalanceStore;
import com.openexchange.oms.ledger.LedgerService;
import com.openexchange.oms.risk.RiskConfig;
import com.openexchange.oms.risk.RiskEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OmsOrderServiceImplTest {

    private OmsOrderServiceImpl orderService;
    private InMemoryBalanceStore balanceStore;
    private ClusterClient clusterClient;
    private RiskEngine riskEngine;
    private OmsMarketDataProvider marketDataProvider;
    private OmsCoreEngine coreEngine;

    @BeforeEach
    void setUp() {
        balanceStore = new InMemoryBalanceStore();
        SnowflakeIdGenerator idGenerator = new SnowflakeIdGenerator(0);
        LedgerService ledgerService = new LedgerService(balanceStore, idGenerator);

        marketDataProvider = new OmsMarketDataProvider();
        OmsBalanceChecker balanceChecker = new OmsBalanceChecker(balanceStore);
        riskEngine = new RiskEngine(6, marketDataProvider, balanceChecker);

        // Configure markets with permissive settings
        RiskConfig config = RiskConfig.builder()
                .minQuantity(1L)
                .maxQuantity(Long.MAX_VALUE)
                .minNotional(0L)
                .maxNotional(Long.MAX_VALUE)
                .priceCollarPercent(50)
                .maxOrdersPerSec(1000)
                .maxOrdersPerMin(10000)
                .maxOpenOrders(1000)
                .maxPositionPerMarket(Long.MAX_VALUE)
                .build();

        for (Market m : Market.ALL) {
            riskEngine.setMarketConfig(m.marketId(), config);
        }

        // Set reference prices so price collar doesn't reject
        marketDataProvider.updateLastTrade(1, FixedPoint.fromDouble(50000.0));
        marketDataProvider.update(1, FixedPoint.fromDouble(49990.0), FixedPoint.fromDouble(50010.0));

        OrderLifecycleManager lcm = new OrderLifecycleManager();
        SyntheticOrderEngine syntheticEngine = new SyntheticOrderEngine();
        coreEngine = new OmsCoreEngine(lcm, syntheticEngine);

        clusterClient = mock(ClusterClient.class);
        when(clusterClient.submitOrder(any(OrderSubmission.class))).thenReturn(true);

        OmsEgressAdapter egressAdapter = new OmsEgressAdapter(coreEngine, marketDataProvider);

        orderService = new OmsOrderServiceImpl(
                coreEngine, riskEngine, ledgerService, clusterClient,
                balanceStore, egressAdapter, idGenerator, marketDataProvider);
    }

    @Test
    void testHappyPathCreateOrder() {
        // Deposit funds
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(100000.0)); // USD

        CreateOrderRequest req = createLimitBuyRequest(1L, 1, 50000.0, 1.0);
        CreateOrderResponse resp = orderService.createOrder(req);

        assertTrue(resp.isAccepted());
        assertTrue(resp.getOmsOrderId() > 0);
        verify(clusterClient).submitOrder(any(OrderSubmission.class));
    }

    @Test
    void testRiskRejection() {
        // Set tight rate limit
        riskEngine.setMarketConfig(1, RiskConfig.builder()
                .minQuantity(1L)
                .maxQuantity(Long.MAX_VALUE)
                .minNotional(0L)
                .maxNotional(Long.MAX_VALUE)
                .priceCollarPercent(50)
                .maxOrdersPerSec(0)  // Reject all
                .maxOrdersPerMin(0)
                .maxOpenOrders(1000)
                .maxPositionPerMarket(Long.MAX_VALUE)
                .build());

        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(100000.0));
        CreateOrderRequest req = createLimitBuyRequest(1L, 1, 50000.0, 1.0);
        CreateOrderResponse resp = orderService.createOrder(req);

        assertFalse(resp.isAccepted());
        assertEquals("RATE_LIMIT_EXCEEDED", resp.getRejectReason());
        verify(clusterClient, never()).submitOrder(any());
    }

    @Test
    void testInsufficientBalance() {
        // No deposits — balance is 0
        CreateOrderRequest req = createLimitBuyRequest(1L, 1, 50000.0, 1.0);
        CreateOrderResponse resp = orderService.createOrder(req);

        assertFalse(resp.isAccepted());
        assertEquals("INSUFFICIENT_BALANCE", resp.getRejectReason());
        verify(clusterClient, never()).submitOrder(any());
    }

    // ---- market/synthetic buy hold estimation (oms#81) ----
    //
    // MARKET, STOP_LOSS and TRAILING_STOP orders carry price=0 (the "unset" convention)
    // until an effective price is estimated from best ask. Before the oms#81 fix, that
    // estimate ran AFTER the ledger hold was placed, so holdForOrder always computed
    // holdAmount = FixedPoint.multiply(0, quantity) = 0, hit LedgerService's
    // holdAmount<=0 guard, and rejected every BUY of these types as "Insufficient
    // balance" regardless of funds. These tests exercise the previously-broken
    // creation path end to end.

    @Test
    void testMarketBuyAcceptedWithSufficientFunds() {
        // Plenty of USD; setUp() configures market 1's best ask at 50010.0.
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(1000000.0));

        CreateOrderRequest req = createMarketBuyRequest(1L, 1, 1.0);
        CreateOrderResponse resp = orderService.createOrder(req);

        assertTrue(resp.isAccepted(),
                "market buy on a funded account must be accepted: " + resp.getRejectReason());
        verify(clusterClient).submitOrder(any(OrderSubmission.class));

        // The hold must be sized off the best-ask-derived estimate (bestAsk * 1.05
        // slippage collar), not zero.
        long expectedEstimatedPrice = (long) (FixedPoint.fromDouble(50010.0) * 1.05);
        long expectedHold = FixedPoint.multiply(expectedEstimatedPrice, FixedPoint.fromDouble(1.0));

        assertTrue(expectedHold > 0);
        assertEquals(expectedHold, balanceStore.getLocked(1L, 0));
        assertEquals(FixedPoint.fromDouble(1000000.0) - expectedHold, balanceStore.getAvailable(1L, 0));
    }

    @Test
    void testMarketBuyRejectedWhenNoBestAsk() {
        // Market 2 has no best ask configured in setUp() — the estimate is unavailable.
        // Must still reject (never hold 0 and pass), and must not touch the balance.
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(1000000.0));

        CreateOrderRequest req = createMarketBuyRequest(1L, 2, 1.0);
        CreateOrderResponse resp = orderService.createOrder(req);

        assertFalse(resp.isAccepted());
        verify(clusterClient, never()).submitOrder(any());
        assertEquals(0L, balanceStore.getLocked(1L, 0));
        assertEquals(FixedPoint.fromDouble(1000000.0), balanceStore.getAvailable(1L, 0));
    }

    @Test
    void testStopLossBuyAcceptedWithSufficientFunds() {
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(1000000.0));

        CreateOrderRequest req = createStopTriggerBuyRequest(1L, 1, "STOP_LOSS", 1.0, 51000.0);
        CreateOrderResponse resp = orderService.createOrder(req);

        assertTrue(resp.isAccepted(),
                "stop-loss buy on a funded account must be accepted: " + resp.getRejectReason());
        // Synthetic orders are held pending trigger, never sent straight to the cluster.
        verify(clusterClient, never()).submitOrder(any());

        assertTrue(balanceStore.getLocked(1L, 0) > 0, "stop-loss buy must place a non-zero hold");

        com.openexchange.oms.common.domain.OmsOrder order =
                coreEngine.getLifecycleManager().getOrder(resp.getOmsOrderId());
        assertNotNull(order);
        assertEquals("PENDING_TRIGGER", order.getStatus().name());
    }

    @Test
    void testTrailingStopBuyAcceptedWithSufficientFunds() {
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(1000000.0));

        CreateOrderRequest req = new CreateOrderRequest();
        req.setUserId(1L);
        req.setMarketId(1);
        req.setSide("BUY");
        req.setOrderType("TRAILING_STOP");
        req.setQuantity(FixedPoint.fromDouble(1.0));
        req.setTrailingDelta(FixedPoint.fromDouble(500.0));

        CreateOrderResponse resp = orderService.createOrder(req);

        assertTrue(resp.isAccepted(),
                "trailing-stop buy on a funded account must be accepted: " + resp.getRejectReason());
        verify(clusterClient, never()).submitOrder(any());
        assertTrue(balanceStore.getLocked(1L, 0) > 0, "trailing-stop buy must place a non-zero hold");
    }

    @Test
    void testStopLimitBuyUsesSuppliedPriceNotEstimate() {
        // STOP_LIMIT always carries a real user-supplied price — it must NOT be
        // overwritten by the best-ask estimate, and the hold must be sized off it.
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(1000000.0));

        CreateOrderRequest req = createStopTriggerBuyRequest(1L, 1, "STOP_LIMIT", 1.0, 50900.0);
        req.setPrice(FixedPoint.fromDouble(51500.0));

        CreateOrderResponse resp = orderService.createOrder(req);

        assertTrue(resp.isAccepted(), "stop-limit buy must be accepted: " + resp.getRejectReason());

        long expectedHold = FixedPoint.multiply(FixedPoint.fromDouble(51500.0), FixedPoint.fromDouble(1.0));
        assertEquals(expectedHold, balanceStore.getLocked(1L, 0));
    }

    @Test
    void testMarketSellUnaffectedHoldsBaseAssetByQuantity() {
        // SELL orders hold base asset by quantity regardless of price; the oms#81
        // fix only touches the BUY path and must not change this.
        balanceStore.deposit(1L, 1, FixedPoint.fromDouble(5.0)); // BTC

        CreateOrderRequest req = new CreateOrderRequest();
        req.setUserId(1L);
        req.setMarketId(1);
        req.setSide("SELL");
        req.setOrderType("MARKET");
        req.setQuantity(FixedPoint.fromDouble(2.0));

        CreateOrderResponse resp = orderService.createOrder(req);

        assertTrue(resp.isAccepted(), "market sell must be accepted: " + resp.getRejectReason());
        assertEquals(FixedPoint.fromDouble(2.0), balanceStore.getLocked(1L, 1));
    }

    @Test
    void testCancelOrder() {
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(100000.0));
        CreateOrderRequest req = createLimitBuyRequest(1L, 1, 50000.0, 1.0);
        CreateOrderResponse createResp = orderService.createOrder(req);
        assertTrue(createResp.isAccepted());

        // Simulate cluster egress assigning a clusterOrderId; without this the
        // order is "in-flight" and cancel rejects.
        coreEngine.getLifecycleManager().onClusterOrderStatus(
                createResp.getOmsOrderId(), 12345L, 0, FixedPoint.fromDouble(1.0), 0L);

        CancelOrderResponse cancelResp = orderService.cancelOrder(createResp.getOmsOrderId());
        assertTrue(cancelResp.isAccepted());
    }

    @Test
    void testGetBalances() {
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(5000.0)); // USD
        balanceStore.deposit(1L, 1, FixedPoint.fromDouble(2.0));    // BTC

        Map<String, Object> balances = orderService.getBalances(1L);
        assertEquals(1L, balances.get("userId"));
        assertNotNull(balances.get("assets"));
    }

    @Test
    void testDepositAndWithdraw() {
        orderService.deposit(1L, 0, FixedPoint.fromDouble(1000.0));
        assertEquals(FixedPoint.fromDouble(1000.0), balanceStore.getAvailable(1L, 0));

        orderService.withdraw(1L, 0, FixedPoint.fromDouble(300.0));
        assertEquals(FixedPoint.fromDouble(700.0), balanceStore.getAvailable(1L, 0));
    }

    @Test
    void testQueueFullRejectsOrder() {
        when(clusterClient.submitOrder(any())).thenReturn(false);

        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(100000.0));
        CreateOrderRequest req = createLimitBuyRequest(1L, 1, 50000.0, 1.0);
        CreateOrderResponse resp = orderService.createOrder(req);

        assertFalse(resp.isAccepted());
        assertEquals("Order queue full", resp.getRejectReason());
    }

    @Test
    void testInvalidUserId() {
        CreateOrderRequest req = createLimitBuyRequest(0L, 1, 50000.0, 1.0);
        CreateOrderResponse resp = orderService.createOrder(req);
        assertFalse(resp.isAccepted());
        assertEquals("Invalid userId", resp.getRejectReason());
    }

    @Test
    void testInvalidMarketId() {
        CreateOrderRequest req = createLimitBuyRequest(1L, 99, 50000.0, 1.0);
        CreateOrderResponse resp = orderService.createOrder(req);
        assertFalse(resp.isAccepted());
        assertEquals("Invalid marketId", resp.getRejectReason());
    }

    // ---- clientOrderId idempotency (oms#40) ----

    @Test
    void testDuplicateClientOrderIdReturnsExistingOrder() {
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(1000000.0));

        CreateOrderRequest req = createLimitBuyRequest(1L, 1, 50000.0, 1.0);
        req.setClientOrderId("my-order-1");
        CreateOrderResponse first = orderService.createOrder(req);
        assertTrue(first.isAccepted());
        assertFalse(first.isDuplicate());

        CreateOrderRequest retry = createLimitBuyRequest(1L, 1, 50000.0, 1.0);
        retry.setClientOrderId("my-order-1");
        CreateOrderResponse second = orderService.createOrder(retry);

        assertTrue(second.isAccepted());
        assertTrue(second.isDuplicate(), "retry with same clientOrderId must not create a new order");
        assertEquals(first.getOmsOrderId(), second.getOmsOrderId());
        verify(clusterClient, times(1)).submitOrder(any(OrderSubmission.class));
    }

    @Test
    void testClientOrderIdReusableAfterTerminal() {
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(1000000.0));

        CreateOrderRequest req = createLimitBuyRequest(1L, 1, 50000.0, 1.0);
        req.setClientOrderId("my-order-2");
        CreateOrderResponse first = orderService.createOrder(req);
        assertTrue(first.isAccepted());

        // Cluster cancels the order → terminal → the id is free again
        coreEngine.getLifecycleManager().onClusterOrderStatus(
                first.getOmsOrderId(), 12345L, 3, 0L, 0L);

        CreateOrderRequest again = createLimitBuyRequest(1L, 1, 50000.0, 1.0);
        again.setClientOrderId("my-order-2");
        CreateOrderResponse second = orderService.createOrder(again);

        assertTrue(second.isAccepted());
        assertFalse(second.isDuplicate(), "terminal order releases its clientOrderId");
        assertNotEquals(first.getOmsOrderId(), second.getOmsOrderId());
    }

    @Test
    void testClientOrderIdScopedPerUser() {
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(1000000.0));
        balanceStore.deposit(2L, 0, FixedPoint.fromDouble(1000000.0));

        CreateOrderRequest u1 = createLimitBuyRequest(1L, 1, 50000.0, 1.0);
        u1.setClientOrderId("shared-id");
        CreateOrderRequest u2 = createLimitBuyRequest(2L, 1, 50000.0, 1.0);
        u2.setClientOrderId("shared-id");

        CreateOrderResponse r1 = orderService.createOrder(u1);
        CreateOrderResponse r2 = orderService.createOrder(u2);

        assertTrue(r1.isAccepted());
        assertTrue(r2.isAccepted());
        assertFalse(r2.isDuplicate(), "clientOrderId namespace is per-user");
        assertNotEquals(r1.getOmsOrderId(), r2.getOmsOrderId());
    }

    @Test
    void testRejectedOrderReleasesClientOrderId() {
        // First attempt fails on balance (no deposit yet)
        CreateOrderRequest req = createLimitBuyRequest(1L, 1, 50000.0, 1.0);
        req.setClientOrderId("retry-after-reject");
        CreateOrderResponse rejected = orderService.createOrder(req);
        assertFalse(rejected.isAccepted());

        // Funded retry with the same id must go through as a NEW order
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(1000000.0));
        CreateOrderRequest retry = createLimitBuyRequest(1L, 1, 50000.0, 1.0);
        retry.setClientOrderId("retry-after-reject");
        CreateOrderResponse accepted = orderService.createOrder(retry);

        assertTrue(accepted.isAccepted());
        assertFalse(accepted.isDuplicate());
    }

    @Test
    void testOverlongClientOrderIdRejected() {
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(1000000.0));
        CreateOrderRequest req = createLimitBuyRequest(1L, 1, 50000.0, 1.0);
        req.setClientOrderId("x".repeat(65));
        CreateOrderResponse resp = orderService.createOrder(req);
        assertFalse(resp.isAccepted());
        verify(clusterClient, never()).submitOrder(any());
    }

    // ---- history reads without persistence (oms#40) ----

    @Test
    void testHistoryUnavailableWithoutPersistence() {
        assertThrows(IllegalStateException.class, () -> orderService.getOrderHistory(1L, null, 100, 0));
        assertThrows(IllegalStateException.class, () -> orderService.getExecutions(1L, 100, 0));
        assertThrows(IllegalStateException.class, () -> orderService.getPositions(1L));
    }

    @Test
    void testQueryOrdersTerminalStatusWithoutPersistenceIsEmpty() {
        // Terminal statuses never live in the active map; without Postgres the
        // query degrades to the pre-oms#40 behavior (empty), not an error.
        assertTrue(orderService.queryOrders(1L, "FILLED").isEmpty());
    }

    private CreateOrderRequest createLimitBuyRequest(long userId, int marketId, double price, double qty) {
        CreateOrderRequest req = new CreateOrderRequest();
        req.setUserId(userId);
        req.setMarketId(marketId);
        req.setSide("BUY");
        req.setOrderType("LIMIT");
        // DTOs carry fixed-point longs now (oms#39)
        req.setPrice(com.match.domain.FixedPoint.fromDouble(price));
        req.setQuantity(com.match.domain.FixedPoint.fromDouble(qty));
        return req;
    }

    // price is left at 0 (unset) — the wire convention for MARKET orders (oms#81)
    private CreateOrderRequest createMarketBuyRequest(long userId, int marketId, double qty) {
        CreateOrderRequest req = new CreateOrderRequest();
        req.setUserId(userId);
        req.setMarketId(marketId);
        req.setSide("BUY");
        req.setOrderType("MARKET");
        req.setQuantity(com.match.domain.FixedPoint.fromDouble(qty));
        return req;
    }

    // price is left at 0 (unset); callers that need a real post-trigger limit
    // price (STOP_LIMIT) set it explicitly afterwards.
    private CreateOrderRequest createStopTriggerBuyRequest(long userId, int marketId, String orderType,
                                                            double qty, double stopPrice) {
        CreateOrderRequest req = new CreateOrderRequest();
        req.setUserId(userId);
        req.setMarketId(marketId);
        req.setSide("BUY");
        req.setOrderType(orderType);
        req.setQuantity(com.match.domain.FixedPoint.fromDouble(qty));
        req.setStopPrice(com.match.domain.FixedPoint.fromDouble(stopPrice));
        return req;
    }
}
