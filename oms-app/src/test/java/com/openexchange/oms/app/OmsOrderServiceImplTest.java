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
import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.common.enums.OmsOrderType;
import com.openexchange.oms.core.OmsCoreEngine;
import com.openexchange.oms.core.OrderLifecycleManager;
import com.openexchange.oms.core.SyntheticOrderEngine;
import com.openexchange.oms.ledger.InMemoryBalanceStore;
import com.openexchange.oms.ledger.LedgerService;
import com.openexchange.oms.risk.RiskConfig;
import com.openexchange.oms.risk.RiskEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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
    private LedgerService ledgerService;

    @BeforeEach
    void setUp() {
        balanceStore = new InMemoryBalanceStore();
        SnowflakeIdGenerator idGenerator = new SnowflakeIdGenerator(0);
        ledgerService = new LedgerService(balanceStore, idGenerator);

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

        // Wire the cluster submit handler exactly as OmsApplication does, so
        // synthetic/iceberg submissions flow coreEngine → handler → clusterClient
        // (the iceberg first-slice + refill path under test, oms#82).
        coreEngine.setClusterSubmitHandler(new OmsCoreEngine.ClusterSubmitHandler() {
            @Override
            public void submitTriggeredOrder(com.openexchange.oms.common.domain.OmsOrder parentOrder,
                                             OmsOrderType childType, long childPrice) {
                // not exercised here
            }

            @Override
            public void submitIcebergSlice(com.openexchange.oms.common.domain.OmsOrder icebergOrder,
                                           long sliceQuantity) {
                long totalPrice = FixedPoint.multiply(icebergOrder.getPrice(), sliceQuantity);
                com.match.infrastructure.generated.OrderSide sbeSide =
                        icebergOrder.getSide() == com.openexchange.oms.common.enums.OrderSide.BUY
                                ? com.match.infrastructure.generated.OrderSide.BID
                                : com.match.infrastructure.generated.OrderSide.ASK;
                clusterClient.submitOrder(OrderSubmission.createOrder(
                        icebergOrder.getUserId(), icebergOrder.getMarketId(),
                        icebergOrder.getPrice(), sliceQuantity, totalPrice,
                        com.match.infrastructure.generated.OrderType.LIMIT,
                        sbeSide, icebergOrder.getOmsOrderId()));
            }

            @Override
            public void submitCancel(long clusterOrderId, long userId, int marketId) {
                clusterClient.submitOrder(OrderSubmission.cancelOrder(userId, clusterOrderId, marketId));
            }

            @Override
            public void submitOpenOrdersSnapshotRequest(long requestId) {
                clusterClient.submitOrder(OrderSubmission.requestOpenOrdersSnapshot(requestId));
            }
        });

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

    // ---- iceberg first-slice submission (oms#82) ----
    //
    // An ICEBERG is synthetic but has no trigger condition: it must start
    // working slices at creation. Before the fix it was routed to
    // PENDING_TRIGGER with no evaluator, so the full hold locked and nothing
    // ever hit the book — funds stuck until manual cancel. It must now submit
    // its first display slice on creation, via the same handler refills use.

    @Test
    void testIcebergSubmitsFirstDisplaySliceOnCreation() {
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(1000000.0));

        // total 10, display 2 — the first slice must be 2, not 10.
        CreateOrderRequest req = createIcebergBuyRequest(1L, 1, 50000.0, 10.0, 2.0);
        CreateOrderResponse resp = orderService.createOrder(req);

        assertTrue(resp.isAccepted(), "iceberg must be accepted: " + resp.getRejectReason());

        // The first slice went to the cluster (this is the whole bug: nothing
        // used to be submitted), carrying the DISPLAY quantity, not the total.
        ArgumentCaptor<OrderSubmission> captor = ArgumentCaptor.forClass(OrderSubmission.class);
        verify(clusterClient, times(1)).submitOrder(captor.capture());
        OrderSubmission slice = captor.getValue();
        assertEquals(OrderSubmission.Type.CREATE, slice.getType());
        assertEquals(FixedPoint.fromDouble(2.0), slice.getQuantity(),
                "first slice must carry the display quantity, not the total");
        assertEquals(FixedPoint.fromDouble(50000.0), slice.getPrice());
        assertEquals(resp.getOmsOrderId(), slice.getOmsOrderId());

        // The order rests via the ordinary path (PENDING_NEW → NEW on ack), it
        // is NOT parked in PENDING_TRIGGER with the book empty.
        OmsOrder order = coreEngine.getLifecycleManager().getOrder(resp.getOmsOrderId());
        assertNotNull(order);
        assertNotEquals("PENDING_TRIGGER", order.getStatus().name());
        assertEquals("PENDING_NEW", order.getStatus().name());

        // Exactly ONE full-quantity hold: price * total. Submitting the slice
        // must not re-hold (that would lock the funds twice).
        long expectedHold = FixedPoint.multiply(FixedPoint.fromDouble(50000.0), FixedPoint.fromDouble(10.0));
        assertEquals(expectedHold, balanceStore.getLocked(1L, 0),
                "hold must be the full quantity once, never double-held per slice");
    }

    @Test
    void testIcebergDisplayGreaterThanTotalIsSingleFullSlice() {
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(1000000.0));

        // display (20) >= total (5): fully displayed, a single slice = the whole order.
        CreateOrderRequest req = createIcebergBuyRequest(1L, 1, 50000.0, 5.0, 20.0);
        CreateOrderResponse resp = orderService.createOrder(req);

        assertTrue(resp.isAccepted(), "iceberg must be accepted: " + resp.getRejectReason());
        ArgumentCaptor<OrderSubmission> captor = ArgumentCaptor.forClass(OrderSubmission.class);
        verify(clusterClient, times(1)).submitOrder(captor.capture());
        assertEquals(FixedPoint.fromDouble(5.0), captor.getValue().getQuantity(),
                "display >= total collapses to a single full-quantity slice");
    }

    @Test
    void testIcebergMissingDisplayTreatedAsFullyDisplayed() {
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(1000000.0));

        // displayQuantity omitted (0) — treated as fully displayed: one slice = total.
        CreateOrderRequest req = createIcebergBuyRequest(1L, 1, 50000.0, 4.0, 0.0);
        CreateOrderResponse resp = orderService.createOrder(req);

        assertTrue(resp.isAccepted(), "iceberg must be accepted: " + resp.getRejectReason());
        ArgumentCaptor<OrderSubmission> captor = ArgumentCaptor.forClass(OrderSubmission.class);
        verify(clusterClient, times(1)).submitOrder(captor.capture());
        assertEquals(FixedPoint.fromDouble(4.0), captor.getValue().getQuantity(),
                "missing display quantity is fully displayed: one slice = total");
    }

    @Test
    void testIcebergRefillSubmitsNextSliceThroughSamePath() {
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(1000000.0));

        CreateOrderRequest req = createIcebergBuyRequest(1L, 1, 50000.0, 10.0, 2.0);
        CreateOrderResponse resp = orderService.createOrder(req);
        assertTrue(resp.isAccepted());

        // Simulate the first display slice filling: the synthetic engine's
        // refill callback is wired to the SAME coreEngine.submitIcebergSlice
        // path the first slice used, so the second slice must go out too.
        coreEngine.getSyntheticEngine().onIcebergSliceFilled(resp.getOmsOrderId());

        ArgumentCaptor<OrderSubmission> captor = ArgumentCaptor.forClass(OrderSubmission.class);
        // 1 first slice + 1 refill slice.
        verify(clusterClient, times(2)).submitOrder(captor.capture());
        OrderSubmission secondSlice = captor.getAllValues().get(1);
        assertEquals(FixedPoint.fromDouble(2.0), secondSlice.getQuantity(),
                "refill slice must carry the display quantity");
        assertEquals(resp.getOmsOrderId(), secondSlice.getOmsOrderId(),
                "refill reuses the parent omsOrderId for egress correlation");
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

    // ---- queue-full leaves no PENDING_NEW zombie + no double-release (oms#85) ----
    //
    // createOrder advances the order to PENDING_NEW before it enqueues to the cluster.
    // On a queue-full enqueue failure the old path called onHoldFailed (guarded on
    // PENDING_HOLD → no-op), so the client was told "rejected" while GET/cancel still saw
    // an active PENDING_NEW order for ~10-20s until the orphan sweep. The fix terminalizes
    // it via onSubmitFailed and lets the state listener perform the SINGLE hold release +
    // slot close. These tests wire that listener exactly as OmsApplication does.

    @Test
    void testQueueFullTerminalizesOrderNoZombieAndReleasesHoldExactlyOnce() {
        wireStateListener();
        when(clusterClient.submitOrder(any())).thenReturn(false);

        long deposit = FixedPoint.fromDouble(100000.0);
        balanceStore.deposit(1L, 0, deposit);

        CreateOrderRequest req = createLimitBuyRequest(1L, 1, 50000.0, 1.0);
        CreateOrderResponse resp = orderService.createOrder(req);

        // Client is told rejected...
        assertFalse(resp.isAccepted());
        assertEquals("Order queue full", resp.getRejectReason());

        // ...and OMS state matches immediately: the order is gone from the active map, so no
        // PENDING_NEW zombie for GET to list or cancel to refuse (oms#85).
        assertNull(coreEngine.getLifecycleManager().getOrder(resp.getOmsOrderId()),
                "queue-full order must not linger as an active PENDING_NEW zombie");

        // The hold is released EXACTLY once: locked back to 0, available back to the FULL
        // deposit — never MORE than the deposit (a double-release would over-credit).
        assertEquals(0L, balanceStore.getLocked(1L, 0));
        assertEquals(deposit, balanceStore.getAvailable(1L, 0));

        // Open-order slot is net-zero: the pre-submit onOrderOpened is balanced by the
        // listener's onOrderClosed on the reject (baseline back to 0).
        assertEquals(0L, riskEngine.getOpenOrderCount(1L));
    }

    @Test
    void testQueueFullDoesNotDoubleReleaseAgainstOtherLockedFunds() {
        wireStateListener();

        long deposit = FixedPoint.fromDouble(200000.0);
        balanceStore.deposit(1L, 0, deposit);

        // A first order rests and keeps its hold locked (submit succeeds, default mock).
        CreateOrderResponse resting =
                orderService.createOrder(createLimitBuyRequest(1L, 1, 50000.0, 1.0));
        assertTrue(resting.isAccepted());
        long restingHold =
                FixedPoint.multiply(FixedPoint.fromDouble(50000.0), FixedPoint.fromDouble(1.0));
        assertEquals(restingHold, balanceStore.getLocked(1L, 0));

        // The next create queue-fulls. Its own hold (another 50k) was placed at step 4 and
        // must be released EXACTLY once. If the queue-full path released explicitly AND let the
        // listener release, the second release would succeed against the RESTING order's locked
        // funds — unlocking money that must stay held (the oms#85 double-release trap).
        when(clusterClient.submitOrder(any())).thenReturn(false);
        CreateOrderResponse rejected =
                orderService.createOrder(createLimitBuyRequest(1L, 1, 50000.0, 1.0));
        assertFalse(rejected.isAccepted());

        // Only the resting order's hold remains locked; available is exactly deposit -
        // restingHold, never MORE (no money created).
        assertEquals(restingHold, balanceStore.getLocked(1L, 0));
        assertEquals(deposit - restingHold, balanceStore.getAvailable(1L, 0));

        // The resting order is untouched and live; the rejected one is gone.
        assertNotNull(coreEngine.getLifecycleManager().getOrder(resting.getOmsOrderId()));
        assertNull(coreEngine.getLifecycleManager().getOrder(rejected.getOmsOrderId()));

        // Slot count reflects only the one surviving open order.
        assertEquals(1L, riskEngine.getOpenOrderCount(1L));
    }

    /**
     * Wire the terminal hold-release + open-order-slot listener exactly as
     * OmsApplication does (release + onOrderClosed on any HELD→terminal transition),
     * minus the WS/gRPC push that has no harness here.
     */
    private void wireStateListener() {
        coreEngine.getLifecycleManager().setStateListener((order, oldStatus, newStatus) -> {
            boolean heldResources = oldStatus == OmsOrderStatus.NEW
                    || oldStatus == OmsOrderStatus.PARTIALLY_FILLED
                    || oldStatus == OmsOrderStatus.PENDING_NEW
                    || oldStatus == OmsOrderStatus.PENDING_TRIGGER;
            boolean releasingTerminal = newStatus == OmsOrderStatus.CANCELLED
                    || newStatus == OmsOrderStatus.EXPIRED
                    || newStatus == OmsOrderStatus.REJECTED;
            if (heldResources && releasingTerminal) {
                ledgerService.releaseForCancel(order);
                riskEngine.onOrderClosed(order.getUserId());
            }
        });
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

    private CreateOrderRequest createIcebergBuyRequest(long userId, int marketId, double price,
                                                       double qty, double displayQty) {
        CreateOrderRequest req = new CreateOrderRequest();
        req.setUserId(userId);
        req.setMarketId(marketId);
        req.setSide("BUY");
        req.setOrderType("ICEBERG");
        req.setPrice(com.match.domain.FixedPoint.fromDouble(price));
        req.setQuantity(com.match.domain.FixedPoint.fromDouble(qty));
        // displayQty <= 0 leaves displayQuantity unset (the "fully displayed" convention).
        if (displayQty > 0) {
            req.setDisplayQuantity(com.match.domain.FixedPoint.fromDouble(displayQty));
        }
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

    // ==================== PUT amend / cancel-and-replace (oms#67) ====================

    /** Wire the replace ledger hooks exactly as OmsApplication does. */
    private void wireReplaceHooks() {
        coreEngine.getLifecycleManager().setReplaceHooks(new OrderLifecycleManager.ReplaceHooks() {
            @Override
            public void onReplaceResolved(OmsOrder order) {
                long target = order.getPendingHoldTarget();
                long surplus = order.getHoldAmount() + order.getPendingHoldDelta() - target;
                if (surplus > 0) {
                    ledgerService.releaseAmendDelta(order, surplus);
                }
                order.setHoldAmount(target);
            }

            @Override
            public void onReplaceAborted(OmsOrder order) {
                if (order.getPendingHoldDelta() > 0) {
                    ledgerService.releaseAmendDelta(order, order.getPendingHoldDelta());
                }
            }
        });
    }

    /** Create a limit buy and walk it to NEW on cluster leg {@code cid}. */
    private OmsOrder restingBuy(long userId, double price, double qty, long cid) {
        CreateOrderResponse resp = orderService.createOrder(createLimitBuyRequest(userId, 1, price, qty));
        assertTrue(resp.isAccepted());
        long omsId = resp.getOmsOrderId();
        coreEngine.getLifecycleManager().onClusterOrderStatus(
                omsId, cid, 0, FixedPoint.fromDouble(qty), 0);
        return coreEngine.getLifecycleManager().getOrder(omsId);
    }

    @Test
    void testAmendGrowLocksDeltaAtSubmitAndResolvesHoldTarget() {
        wireReplaceHooks();
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(200_000.0));
        OmsOrder order = restingBuy(1L, 50_000.0, 1.0, 900L);
        assertEquals(FixedPoint.fromDouble(50_000.0), balanceStore.getLocked(1L, 0));

        Map<String, Object> resp = orderService.updateOrder(
                order.getOmsOrderId(), FixedPoint.fromDouble(60_000.0), 0);
        assertEquals(Boolean.TRUE, resp.get("accepted"));
        // The +10k delta is locked at submit (the amend's funds check).
        assertEquals(FixedPoint.fromDouble(60_000.0), balanceStore.getLocked(1L, 0));
        assertTrue(order.isReplacePending());

        // The leg submitted to the engine carries the remaining quantity at the new price.
        ArgumentCaptor<OrderSubmission> captor = ArgumentCaptor.forClass(OrderSubmission.class);
        verify(clusterClient, atLeast(2)).submitOrder(captor.capture());
        OrderSubmission update = captor.getAllValues().get(captor.getAllValues().size() - 1);
        assertEquals(FixedPoint.fromDouble(60_000.0), update.getPrice());
        assertEquals(FixedPoint.fromDouble(1.0), update.getQuantity());

        // Resolution installs the target hold; nothing further moves in the ledger.
        coreEngine.getLifecycleManager().onClusterOrderStatus(
                order.getOmsOrderId(), 901L, 0, FixedPoint.fromDouble(1.0), 0);
        assertEquals(FixedPoint.fromDouble(60_000.0), order.getHoldAmount());
        assertEquals(FixedPoint.fromDouble(60_000.0), balanceStore.getLocked(1L, 0));
        assertEquals(FixedPoint.fromDouble(60_000.0), order.getPrice());
        assertFalse(order.isReplacePending());
    }

    @Test
    void testAmendShrinkReleasesOnlyAtResolution() {
        wireReplaceHooks();
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(100_000.0));
        OmsOrder order = restingBuy(1L, 50_000.0, 1.0, 910L);

        Map<String, Object> resp = orderService.updateOrder(
                order.getOmsOrderId(), FixedPoint.fromDouble(40_000.0), 0);
        assertEquals(Boolean.TRUE, resp.get("accepted"));
        // Shrink: nothing released at submit — the amend may still fail.
        assertEquals(FixedPoint.fromDouble(50_000.0), balanceStore.getLocked(1L, 0));

        coreEngine.getLifecycleManager().onClusterOrderStatus(
                order.getOmsOrderId(), 911L, 0, FixedPoint.fromDouble(1.0), 0);
        // Resolution releases the 10k surplus.
        assertEquals(FixedPoint.fromDouble(40_000.0), balanceStore.getLocked(1L, 0));
        assertEquals(FixedPoint.fromDouble(40_000.0), order.getHoldAmount());
    }

    @Test
    void testAmendInsufficientBalanceRejectedAndUnwound() {
        wireReplaceHooks();
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(50_000.0)); // exactly the original hold
        OmsOrder order = restingBuy(1L, 50_000.0, 1.0, 920L);
        assertEquals(0L, balanceStore.getAvailable(1L, 0));

        Map<String, Object> resp = orderService.updateOrder(
                order.getOmsOrderId(), FixedPoint.fromDouble(60_000.0), 0);
        assertEquals(Boolean.FALSE, resp.get("accepted"));
        assertEquals("Insufficient balance for amend", resp.get("message"));
        assertFalse(order.isReplacePending(), "marker rolled back");
        assertEquals(FixedPoint.fromDouble(50_000.0), balanceStore.getLocked(1L, 0));
        assertEquals(0L, balanceStore.getAvailable(1L, 0));
        // Only the original create reached the cluster.
        verify(clusterClient, times(1)).submitOrder(any());
    }

    @Test
    void testAmendQueueFullRollsBackDeltaAndMarker() {
        wireReplaceHooks();
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(200_000.0));
        OmsOrder order = restingBuy(1L, 50_000.0, 1.0, 930L);

        when(clusterClient.submitOrder(any(OrderSubmission.class))).thenReturn(false);
        Map<String, Object> resp = orderService.updateOrder(
                order.getOmsOrderId(), FixedPoint.fromDouble(60_000.0), 0);
        assertEquals(Boolean.FALSE, resp.get("accepted"));
        assertEquals("Order queue full", resp.get("message"));
        assertFalse(order.isReplacePending());
        // The +10k delta locked before the enqueue is released again.
        assertEquals(FixedPoint.fromDouble(50_000.0), balanceStore.getLocked(1L, 0));
        assertEquals(FixedPoint.fromDouble(150_000.0), balanceStore.getAvailable(1L, 0));
    }

    @Test
    void testAmendBelowFilledQuantityRejected() {
        wireReplaceHooks();
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(100_000.0));
        OmsOrder order = restingBuy(1L, 50_000.0, 1.0, 940L);
        order.setFilledQty(FixedPoint.fromDouble(0.6));

        Map<String, Object> resp = orderService.updateOrder(
                order.getOmsOrderId(), 0, FixedPoint.fromDouble(0.5));
        assertEquals(Boolean.FALSE, resp.get("accepted"));
        assertFalse(order.isReplacePending());
    }

    @Test
    void testAmendPartiallyFilledSubmitsRemainingLeg() {
        wireReplaceHooks();
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(200_000.0));
        OmsOrder order = restingBuy(1L, 50_000.0, 1.0, 950L);
        order.setFilledQty(FixedPoint.fromDouble(0.4));

        Map<String, Object> resp = orderService.updateOrder(
                order.getOmsOrderId(), FixedPoint.fromDouble(55_000.0), 0);
        assertEquals(Boolean.TRUE, resp.get("accepted"));

        ArgumentCaptor<OrderSubmission> captor = ArgumentCaptor.forClass(OrderSubmission.class);
        verify(clusterClient, atLeast(2)).submitOrder(captor.capture());
        OrderSubmission update = captor.getAllValues().get(captor.getAllValues().size() - 1);
        // Only the unfilled 0.6 goes to the engine — the replacement leg rests its full
        // submitted quantity, so prior fills must be subtracted here.
        assertEquals(FixedPoint.fromDouble(0.6), update.getQuantity());
    }

    @Test
    void testAmendNonLimitRejected() {
        wireReplaceHooks();
        balanceStore.deposit(1L, 0, FixedPoint.fromDouble(100_000.0));
        OmsOrder order = restingBuy(1L, 50_000.0, 1.0, 960L);
        order.setOrderType(OmsOrderType.ICEBERG);

        Map<String, Object> resp = orderService.updateOrder(
                order.getOmsOrderId(), FixedPoint.fromDouble(55_000.0), 0);
        assertEquals(Boolean.FALSE, resp.get("accepted"));
        assertFalse(order.isReplacePending());
    }
}
