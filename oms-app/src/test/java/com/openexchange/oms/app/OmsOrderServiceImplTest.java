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

    private CreateOrderRequest createLimitBuyRequest(long userId, int marketId, double price, double qty) {
        CreateOrderRequest req = new CreateOrderRequest();
        req.setUserId(userId);
        req.setMarketId(marketId);
        req.setSide("BUY");
        req.setOrderType("LIMIT");
        req.setPrice(price);
        req.setQuantity(qty);
        return req;
    }
}
