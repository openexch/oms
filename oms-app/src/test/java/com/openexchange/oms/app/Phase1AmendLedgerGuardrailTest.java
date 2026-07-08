// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.match.domain.FixedPoint;
import com.openexchange.oms.api.dto.CreateOrderRequest;
import com.openexchange.oms.api.dto.CreateOrderResponse;
import com.openexchange.oms.cluster.ClusterClient;
import com.openexchange.oms.cluster.OrderSubmission;
import com.openexchange.oms.common.domain.Market;
import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.domain.SnowflakeIdGenerator;
import com.openexchange.oms.common.enums.OmsOrderStatus;
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

/**
 * Phase-1 money-state GUARDRAIL — amend hold arithmetic + iceberg backpressure
 * (OMS-3, OMS-8), end-to-end through the real ledger. Reproduces confirmed
 * fund-integrity defects deterministically; the regression gate for the fixes.
 * FAILS against the pre-fix code by design.
 * <p>
 * Assertions are on the OBSERVABLE money invariant (funds conserved, no stranded
 * or double-locked balance), not on internal method returns, so they survive
 * whichever layer the fix lands in.
 * <p>
 * Wiring mirrors {@code OmsOrderServiceImplTest} (only ClusterClient is mocked).
 */
class Phase1AmendLedgerGuardrailTest {

    private OmsOrderServiceImpl orderService;
    private InMemoryBalanceStore balanceStore;
    private ClusterClient clusterClient;
    private OmsCoreEngine coreEngine;
    private LedgerService ledgerService;

    @BeforeEach
    void setUp() {
        balanceStore = new InMemoryBalanceStore();
        SnowflakeIdGenerator idGenerator = new SnowflakeIdGenerator(0);
        ledgerService = new LedgerService(balanceStore, idGenerator);

        OmsMarketDataProvider marketDataProvider = new OmsMarketDataProvider();
        OmsBalanceChecker balanceChecker = new OmsBalanceChecker(balanceStore);
        RiskEngine riskEngine = new RiskEngine(6, marketDataProvider, balanceChecker);

        RiskConfig config = RiskConfig.builder()
                .minQuantity(1L).maxQuantity(Long.MAX_VALUE)
                .minNotional(0L).maxNotional(Long.MAX_VALUE)
                .priceCollarPercent(50)
                .maxOrdersPerSec(1000).maxOrdersPerMin(10000)
                .maxOpenOrders(1000).maxPositionPerMarket(Long.MAX_VALUE)
                .build();
        for (Market m : Market.ALL) {
            riskEngine.setMarketConfig(m.marketId(), config);
        }
        marketDataProvider.updateLastTrade(1, FixedPoint.fromDouble(50000.0));
        marketDataProvider.update(1, FixedPoint.fromDouble(49990.0), FixedPoint.fromDouble(50010.0));

        OrderLifecycleManager lcm = new OrderLifecycleManager();
        SyntheticOrderEngine syntheticEngine = new SyntheticOrderEngine();
        coreEngine = new OmsCoreEngine(lcm, syntheticEngine);

        clusterClient = mock(ClusterClient.class);
        when(clusterClient.submitOrder(any(OrderSubmission.class))).thenReturn(true);

        // Cluster submit handler exactly as OmsApplication wires it (iceberg/synthetic
        // submissions flow coreEngine → handler → clusterClient), so OMS-8's dropped
        // submitOrder boolean is exercised on the real path.
        coreEngine.setClusterSubmitHandler(new OmsCoreEngine.ClusterSubmitHandler() {
            @Override
            public void submitTriggeredOrder(OmsOrder parentOrder,
                                             com.openexchange.oms.common.enums.OmsOrderType childType, long childPrice) { }

            @Override
            public boolean submitIcebergSlice(OmsOrder icebergOrder, long sliceQuantity) {
                long totalPrice = FixedPoint.multiply(icebergOrder.getPrice(), sliceQuantity);
                com.match.infrastructure.generated.OrderSide sbeSide =
                        icebergOrder.getSide() == com.openexchange.oms.common.enums.OrderSide.BUY
                                ? com.match.infrastructure.generated.OrderSide.BID
                                : com.match.infrastructure.generated.OrderSide.ASK;
                return clusterClient.submitOrder(OrderSubmission.createOrder(
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

        // Terminal hold-release + amend hooks, wired exactly as OmsApplication does.
        lcm.setStateListener((order, oldStatus, newStatus) -> {
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
        // Delegate to the production ledger methods exactly as OmsApplication does, so this
        // guardrail exercises (and gates) the real amend-resolution math.
        lcm.setReplaceHooks(new OrderLifecycleManager.ReplaceHooks() {
            @Override
            public void onReplaceResolved(OmsOrder order) {
                ledgerService.resolveAmendHold(order);
            }

            @Override
            public void onReplaceAborted(OmsOrder order) {
                ledgerService.abortAmendHold(order);
            }
        });
    }

    private OmsOrder restingBuy(long userId, double price, double qty, long cid) {
        CreateOrderRequest req = new CreateOrderRequest();
        req.setUserId(userId);
        req.setMarketId(1);
        req.setSide("BUY");
        req.setOrderType("LIMIT");
        req.setPrice(FixedPoint.fromDouble(price));
        req.setQuantity(FixedPoint.fromDouble(qty));
        CreateOrderResponse resp = orderService.createOrder(req);
        assertTrue(resp.isAccepted(), "resting buy must be accepted: " + resp.getRejectReason());
        long omsId = resp.getOmsOrderId();
        coreEngine.getLifecycleManager().onClusterOrderStatus(omsId, cid, 0, FixedPoint.fromDouble(qty), 0);
        return coreEngine.getLifecycleManager().getOrder(omsId);
    }

    /**
     * OMS-3 — a price-lowering amend after a partial fill over-releases on cancel.
     * {@code computeAmendHoldTarget} sizes the hold off the full amended quantity, not
     * netting the already-filled qty (unlike releaseForCancel's − price·filledQty). After
     * create → partial fill → price-down amend, the recorded holdAmount exceeds what is
     * actually locked; on cancel {@code releaseForCancel} requests more than remains and the
     * all-or-nothing {@code release} FAILS entirely — stranding the user's funds in locked.
     * <p>
     * Invariant (funds conserved): after the sequence the user has paid exactly
     * price·filledQty; locked returns to 0 and available returns to deposit − price·filledQty.
     */
    @Test
    void oms3_partialFillThenPriceLoweringAmendConservesFunds() {
        final int USD = 0, BTC = 1;
        final long deposit = FixedPoint.fromDouble(1_000_000.0);
        balanceStore.deposit(1L, USD, deposit);
        // A counterparty seller holding base so the trade settles cleanly (no over-settle noise).
        balanceStore.deposit(2L, BTC, FixedPoint.fromDouble(0.4));
        balanceStore.hold(2L, BTC, FixedPoint.fromDouble(0.4), 999L);

        // Resting BUY 1.0 @ 50_000 → locked quote = 50_000.
        OmsOrder order = restingBuy(1L, 50_000.0, 1.0, 900L);
        assertEquals(FixedPoint.fromDouble(50_000.0), balanceStore.getLocked(1L, USD));

        // Partial fill 0.4 @ 50_000 settles: buyer locked quote −= 20_000, buyer base += 0.4.
        long fillQty = FixedPoint.fromDouble(0.4);
        ledgerService.settleTradeExecution(1L, /*buyer*/ 1L, /*seller*/ 2L, 1,
                FixedPoint.fromDouble(50_000.0), fillQty, order.getOmsOrderId(), 999L);
        order.setFilledQty(fillQty);
        order.setRemainingQty(order.getQuantity() - fillQty);
        assertEquals(FixedPoint.fromDouble(30_000.0), balanceStore.getLocked(1L, USD),
                "post-fill locked = price·(qty − filled)");

        // Amend the price DOWN to 40_000 (same total qty), then resolve, then cancel.
        Map<String, Object> resp = orderService.updateOrder(order.getOmsOrderId(), FixedPoint.fromDouble(40_000.0), 0);
        assertEquals(Boolean.TRUE, resp.get("accepted"), "price-lowering amend must be accepted");
        coreEngine.getLifecycleManager().onClusterOrderStatus(
                order.getOmsOrderId(), 901L, 0, order.getRemainingQty(), order.getFilledQty()); // resolve
        coreEngine.getLifecycleManager().onClusterOrderStatus(
                order.getOmsOrderId(), 901L, 3, 0, 0); // CANCELLED

        long spent = FixedPoint.multiply(FixedPoint.fromDouble(50_000.0), fillQty); // paid 20_000 for 0.4 BTC
        assertEquals(0L, balanceStore.getLocked(1L, USD),
                "no quote may be stranded in locked after cancel");
        assertEquals(deposit - spent, balanceStore.getAvailable(1L, USD),
                "user paid exactly price·filledQty; every other quote unit must be back in available");
    }

    /**
     * OMS-8 — an iceberg whose FIRST slice hits a full cluster queue is silently accepted.
     * The iceberg submit path discards {@code submitOrder}'s boolean (unlike the normal
     * create path, which rolls back + rejects on a full queue). So the order is left
     * PENDING_NEW with the full hold locked and nothing on the book, yet createOrder returns
     * accepted — funds stuck until a manual cancel.
     * <p>
     * Invariant: a queue-full first slice is rejected and its hold released — parity with the
     * normal create path (testQueueFullRejectsOrder).
     */
    @Test
    void oms8_icebergFirstSliceUnderFullQueueIsRejectedNotStuck() {
        final int USD = 0;
        final long deposit = FixedPoint.fromDouble(1_000_000.0);
        balanceStore.deposit(1L, USD, deposit);
        when(clusterClient.submitOrder(any())).thenReturn(false); // cluster queue full

        CreateOrderRequest req = new CreateOrderRequest();
        req.setUserId(1L);
        req.setMarketId(1);
        req.setSide("BUY");
        req.setOrderType("ICEBERG");
        req.setPrice(FixedPoint.fromDouble(50_000.0));
        req.setQuantity(FixedPoint.fromDouble(10.0));
        req.setDisplayQuantity(FixedPoint.fromDouble(2.0));
        CreateOrderResponse resp = orderService.createOrder(req);

        assertFalse(resp.isAccepted(),
                "an iceberg whose first slice hit a full queue must be rejected, not accepted");
        assertNull(coreEngine.getLifecycleManager().getOrder(resp.getOmsOrderId()),
                "no PENDING_NEW zombie may linger with a full hold and nothing on the book");
        assertEquals(0L, balanceStore.getLocked(1L, USD),
                "the hold must be released when the first slice cannot enqueue");
        assertEquals(deposit, balanceStore.getAvailable(1L, USD));
    }
}
