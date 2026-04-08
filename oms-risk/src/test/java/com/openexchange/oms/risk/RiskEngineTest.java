package com.openexchange.oms.risk;

import com.match.domain.FixedPoint;
import com.openexchange.oms.common.domain.RiskRejectReason;
import com.openexchange.oms.common.enums.OmsOrderType;
import com.openexchange.oms.common.enums.OrderSide;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RiskEngineTest {

    private RiskEngine riskEngine;
    private TestMarketDataProvider marketData;
    private TestBalanceChecker balanceChecker;

    private static final long USER_ID = 100L;
    private static final int MARKET_ID = 1;
    private static final long PRICE = FixedPoint.fromDouble(50000.0);
    private static final long QTY = FixedPoint.fromDouble(1.0);

    @BeforeEach
    void setUp() {
        marketData = new TestMarketDataProvider();
        balanceChecker = new TestBalanceChecker();
        riskEngine = new RiskEngine(6, marketData, balanceChecker);

        // Default config
        riskEngine.setMarketConfig(MARKET_ID, RiskConfig.builder()
                .minQuantity(FixedPoint.fromDouble(0.001))
                .maxQuantity(FixedPoint.fromDouble(100.0))
                .minNotional(FixedPoint.fromDouble(1.0))
                .maxNotional(FixedPoint.fromDouble(10_000_000.0))
                .priceCollarPercent(10)
                .maxOrdersPerSec(5)
                .maxOrdersPerMin(100)
                .maxOpenOrders(10)
                .maxPositionPerMarket(FixedPoint.fromDouble(50.0))
                .build());

        // Set market data
        marketData.lastTradePrice = PRICE;
        marketData.bestBid = PRICE - FixedPoint.fromDouble(10.0);
        marketData.bestAsk = PRICE + FixedPoint.fromDouble(10.0);

        // Sufficient balance by default
        balanceChecker.sufficient = true;
    }

    @Test
    void testHappyPath() {
        RiskResult result = riskEngine.check(USER_ID, MARKET_ID, OrderSide.BUY, OmsOrderType.LIMIT, PRICE, QTY);
        assertTrue(result.isPassed());
    }

    // -- Rate limiter --

    @Test
    void testRateLimiterRejects() {
        // Config allows 5 per second
        for (int i = 0; i < 5; i++) {
            RiskResult r = riskEngine.check(USER_ID, MARKET_ID, OrderSide.BUY, OmsOrderType.LIMIT, PRICE, QTY);
            assertTrue(r.isPassed(), "Order " + i + " should pass");
        }
        // 6th should be rejected
        RiskResult result = riskEngine.check(USER_ID, MARKET_ID, OrderSide.BUY, OmsOrderType.LIMIT, PRICE, QTY);
        assertFalse(result.isPassed());
        assertEquals(RiskRejectReason.RATE_LIMIT_EXCEEDED, result.getRejectReason());
    }

    // -- Circuit breaker --

    @Test
    void testCircuitBreakerTrip() {
        riskEngine.tripCircuitBreaker(MARKET_ID);
        RiskResult result = riskEngine.check(USER_ID, MARKET_ID, OrderSide.BUY, OmsOrderType.LIMIT, PRICE, QTY);
        assertFalse(result.isPassed());
        assertEquals(RiskRejectReason.CIRCUIT_BREAKER_OPEN, result.getRejectReason());
    }

    @Test
    void testCircuitBreakerReset() {
        riskEngine.tripCircuitBreaker(MARKET_ID);
        riskEngine.resetCircuitBreaker(MARKET_ID);
        RiskResult result = riskEngine.check(USER_ID, MARKET_ID, OrderSide.BUY, OmsOrderType.LIMIT, PRICE, QTY);
        assertTrue(result.isPassed());
    }

    // -- Order size --

    @Test
    void testOrderSizeTooSmall() {
        long tinyQty = FixedPoint.fromDouble(0.0001); // below min 0.001
        RiskResult result = riskEngine.check(USER_ID, MARKET_ID, OrderSide.BUY, OmsOrderType.LIMIT, PRICE, tinyQty);
        assertFalse(result.isPassed());
        assertEquals(RiskRejectReason.ORDER_SIZE_TOO_SMALL, result.getRejectReason());
    }

    @Test
    void testOrderSizeTooLarge() {
        long hugeQty = FixedPoint.fromDouble(200.0); // above max 100.0
        RiskResult result = riskEngine.check(USER_ID, MARKET_ID, OrderSide.BUY, OmsOrderType.LIMIT, PRICE, hugeQty);
        assertFalse(result.isPassed());
        assertEquals(RiskRejectReason.ORDER_SIZE_TOO_LARGE, result.getRejectReason());
    }

    // -- Price collar --

    @Test
    void testPriceCollarRejected() {
        // 20% off last trade should be rejected (collar is 10%)
        long farPrice = FixedPoint.fromDouble(40000.0); // 20% below 50000
        RiskResult result = riskEngine.check(USER_ID, MARKET_ID, OrderSide.BUY, OmsOrderType.LIMIT, farPrice, QTY);
        assertFalse(result.isPassed());
        assertEquals(RiskRejectReason.PRICE_COLLAR_BREACH, result.getRejectReason());
    }

    @Test
    void testPriceCollarPassed() {
        // 5% off should pass (collar is 10%)
        long closePrice = FixedPoint.fromDouble(47500.0);
        RiskResult result = riskEngine.check(USER_ID, MARKET_ID, OrderSide.BUY, OmsOrderType.LIMIT, closePrice, QTY);
        assertTrue(result.isPassed());
    }

    @Test
    void testPriceCollarSkippedForMarketOrders() {
        RiskResult result = riskEngine.check(USER_ID, MARKET_ID, OrderSide.BUY, OmsOrderType.MARKET, 0L, QTY);
        assertTrue(result.isPassed());
    }

    // -- Open order limit --

    @Test
    void testOpenOrderLimit() {
        // Fill to limit (10)
        for (int i = 0; i < 10; i++) {
            riskEngine.onOrderOpened(USER_ID);
        }
        RiskResult result = riskEngine.check(USER_ID, MARKET_ID, OrderSide.BUY, OmsOrderType.LIMIT, PRICE, QTY);
        assertFalse(result.isPassed());
        assertEquals(RiskRejectReason.OPEN_ORDER_LIMIT, result.getRejectReason());
    }

    @Test
    void testOpenOrderLimitAfterClose() {
        for (int i = 0; i < 10; i++) {
            riskEngine.onOrderOpened(USER_ID);
        }
        riskEngine.onOrderClosed(USER_ID);
        // Reset rate limiter so it doesn't interfere
        riskEngine.resetRateLimits();
        RiskResult result = riskEngine.check(USER_ID, MARKET_ID, OrderSide.BUY, OmsOrderType.LIMIT, PRICE, QTY);
        assertTrue(result.isPassed());
    }

    // -- Balance --

    @Test
    void testInsufficientBalance() {
        balanceChecker.sufficient = false;
        RiskResult result = riskEngine.check(USER_ID, MARKET_ID, OrderSide.BUY, OmsOrderType.LIMIT, PRICE, QTY);
        assertFalse(result.isPassed());
        assertEquals(RiskRejectReason.INSUFFICIENT_BALANCE, result.getRejectReason());
    }

    // -- Position limit --

    @Test
    void testPositionLimitExceeded() {
        // Build up to near max (50 BTC)
        riskEngine.onFill(USER_ID, MARKET_ID, OrderSide.BUY, FixedPoint.fromDouble(49.0));
        // Trying to buy 2 more (total 51) should exceed limit
        long qty2 = FixedPoint.fromDouble(2.0);
        RiskResult result = riskEngine.check(USER_ID, MARKET_ID, OrderSide.BUY, OmsOrderType.LIMIT, PRICE, qty2);
        assertFalse(result.isPassed());
        assertEquals(RiskRejectReason.POSITION_LIMIT_EXCEEDED, result.getRejectReason());
    }

    @Test
    void testPositionLimitOppositeDirection() {
        // Long 49, selling 1 should be fine (position goes to 48)
        riskEngine.onFill(USER_ID, MARKET_ID, OrderSide.BUY, FixedPoint.fromDouble(49.0));
        RiskResult result = riskEngine.check(USER_ID, MARKET_ID, OrderSide.SELL, OmsOrderType.LIMIT, PRICE, QTY);
        assertTrue(result.isPassed());
    }

    // -- Test helpers --

    private static class TestMarketDataProvider implements MarketDataProvider {
        long lastTradePrice;
        long bestBid;
        long bestAsk;

        @Override
        public long getLastTradePrice(int marketId) { return lastTradePrice; }
        @Override
        public long getBestBid(int marketId) { return bestBid; }
        @Override
        public long getBestAsk(int marketId) { return bestAsk; }
    }

    private static class TestBalanceChecker implements BalanceChecker {
        boolean sufficient = true;

        @Override
        public boolean hasSufficientBalance(long userId, int assetId, long amount) {
            return sufficient;
        }
    }
}
