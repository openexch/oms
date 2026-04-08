package com.openexchange.oms.ledger;

import com.match.domain.FixedPoint;
import com.openexchange.oms.common.domain.Market;
import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.domain.SnowflakeIdGenerator;
import com.openexchange.oms.common.enums.OmsOrderType;
import com.openexchange.oms.common.enums.OrderSide;
import com.openexchange.oms.common.enums.TimeInForce;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LedgerServiceTest {

    private InMemoryBalanceStore balanceStore;
    private LedgerService ledgerService;

    private static final long USER_1 = 1L;
    private static final long USER_2 = 2L;
    private static final int BTC_USD_MARKET = 1;

    @BeforeEach
    void setUp() {
        balanceStore = new InMemoryBalanceStore();
        ledgerService = new LedgerService(balanceStore, new SnowflakeIdGenerator(0));
    }

    @Test
    void testHoldForBuyOrder() {
        // Deposit 100,000 USD
        balanceStore.deposit(USER_1, Market.BTC_USD.quoteAsset().id(), FixedPoint.fromDouble(100000.0));

        OmsOrder order = createOrder(USER_1, BTC_USD_MARKET, OrderSide.BUY, FixedPoint.fromDouble(50000.0), FixedPoint.fromDouble(1.0));
        List<LedgerEntry> entries = ledgerService.holdForOrder(order);

        assertFalse(entries.isEmpty());
        assertEquals(2, entries.size());

        // Available should decrease, locked should increase
        long expectedHold = FixedPoint.multiply(FixedPoint.fromDouble(50000.0), FixedPoint.fromDouble(1.0));
        assertEquals(FixedPoint.fromDouble(100000.0) - expectedHold,
                balanceStore.getAvailable(USER_1, Market.BTC_USD.quoteAsset().id()));
        assertEquals(expectedHold,
                balanceStore.getLocked(USER_1, Market.BTC_USD.quoteAsset().id()));
    }

    @Test
    void testHoldInsufficientBalance() {
        // Deposit only 100 USD, try to hold for 50000 * 1.0 = 50000
        balanceStore.deposit(USER_1, Market.BTC_USD.quoteAsset().id(), FixedPoint.fromDouble(100.0));

        OmsOrder order = createOrder(USER_1, BTC_USD_MARKET, OrderSide.BUY, FixedPoint.fromDouble(50000.0), FixedPoint.fromDouble(1.0));
        List<LedgerEntry> entries = ledgerService.holdForOrder(order);

        assertTrue(entries.isEmpty());
        // Balance unchanged
        assertEquals(FixedPoint.fromDouble(100.0), balanceStore.getAvailable(USER_1, Market.BTC_USD.quoteAsset().id()));
    }

    @Test
    void testHoldForSellOrder() {
        // Deposit 10 BTC
        balanceStore.deposit(USER_1, Market.BTC_USD.baseAsset().id(), FixedPoint.fromDouble(10.0));

        OmsOrder order = createOrder(USER_1, BTC_USD_MARKET, OrderSide.SELL, FixedPoint.fromDouble(50000.0), FixedPoint.fromDouble(2.0));
        List<LedgerEntry> entries = ledgerService.holdForOrder(order);

        assertFalse(entries.isEmpty());
        assertEquals(FixedPoint.fromDouble(8.0), balanceStore.getAvailable(USER_1, Market.BTC_USD.baseAsset().id()));
        assertEquals(FixedPoint.fromDouble(2.0), balanceStore.getLocked(USER_1, Market.BTC_USD.baseAsset().id()));
    }

    @Test
    void testReleaseOnCancel() {
        balanceStore.deposit(USER_1, Market.BTC_USD.quoteAsset().id(), FixedPoint.fromDouble(100000.0));

        OmsOrder order = createOrder(USER_1, BTC_USD_MARKET, OrderSide.BUY, FixedPoint.fromDouble(50000.0), FixedPoint.fromDouble(1.0));
        ledgerService.holdForOrder(order);

        // Cancel — no fills
        order.setFilledQty(0L);
        List<LedgerEntry> releaseEntries = ledgerService.releaseForCancel(order);

        assertFalse(releaseEntries.isEmpty());
        assertEquals(FixedPoint.fromDouble(100000.0), balanceStore.getAvailable(USER_1, Market.BTC_USD.quoteAsset().id()));
        assertEquals(0L, balanceStore.getLocked(USER_1, Market.BTC_USD.quoteAsset().id()));
    }

    @Test
    void testSettleTrade() {
        // Buyer holds quote
        long quoteHold = FixedPoint.multiply(FixedPoint.fromDouble(50000.0), FixedPoint.fromDouble(1.0));
        balanceStore.deposit(USER_1, Market.BTC_USD.quoteAsset().id(), quoteHold);
        balanceStore.hold(USER_1, Market.BTC_USD.quoteAsset().id(), quoteHold, 100L);

        // Seller holds base
        balanceStore.deposit(USER_2, Market.BTC_USD.baseAsset().id(), FixedPoint.fromDouble(1.0));
        balanceStore.hold(USER_2, Market.BTC_USD.baseAsset().id(), FixedPoint.fromDouble(1.0), 200L);

        List<LedgerEntry> entries = ledgerService.settleTradeExecution(
                999L, USER_1, USER_2, BTC_USD_MARKET,
                FixedPoint.fromDouble(50000.0), FixedPoint.fromDouble(1.0), 100L, 200L);

        assertEquals(4, entries.size());

        // Buyer gets BTC
        assertEquals(FixedPoint.fromDouble(1.0), balanceStore.getAvailable(USER_1, Market.BTC_USD.baseAsset().id()));
        // Seller gets USD
        assertEquals(quoteHold, balanceStore.getAvailable(USER_2, Market.BTC_USD.quoteAsset().id()));
    }

    @Test
    void testOverlock() {
        // Hold at price 50000, fill at 48000 — should release (50000-48000)*qty
        long holdPrice = FixedPoint.fromDouble(50000.0);
        long fillPrice = FixedPoint.fromDouble(48000.0);
        long qty = FixedPoint.fromDouble(1.0);
        long holdAmt = FixedPoint.multiply(holdPrice, qty);

        balanceStore.deposit(USER_1, Market.BTC_USD.quoteAsset().id(), holdAmt);
        balanceStore.hold(USER_1, Market.BTC_USD.quoteAsset().id(), holdAmt, 100L);

        // The overlock calculation: (holdPrice - fillPrice) * qty
        long priceDelta = holdPrice - fillPrice;
        long expectedOverlock = FixedPoint.multiply(priceDelta, qty);

        List<LedgerEntry> entries = ledgerService.handleOverlock(100L, USER_1, BTC_USD_MARKET, holdPrice, fillPrice, qty);
        assertFalse(entries.isEmpty());

        // The overlock amount should be released from locked to available
        assertEquals(expectedOverlock, balanceStore.getAvailable(USER_1, Market.BTC_USD.quoteAsset().id()));
    }

    private OmsOrder createOrder(long userId, int marketId, OrderSide side, long price, long qty) {
        OmsOrder order = new OmsOrder();
        order.setOmsOrderId(System.nanoTime());
        order.setUserId(userId);
        order.setMarketId(marketId);
        order.setSide(side);
        order.setOrderType(OmsOrderType.LIMIT);
        order.setTimeInForce(TimeInForce.GTC);
        order.setPrice(price);
        order.setQuantity(qty);
        order.setRemainingQty(qty);
        return order;
    }
}
