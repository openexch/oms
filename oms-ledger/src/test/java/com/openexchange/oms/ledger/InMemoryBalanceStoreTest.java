package com.openexchange.oms.ledger;

import com.match.domain.FixedPoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryBalanceStoreTest {

    private InMemoryBalanceStore store;

    private static final long USER_ID = 1L;
    private static final int ASSET_ID = 0; // USD
    private static final long ORDER_ID = 100L;

    @BeforeEach
    void setUp() {
        store = new InMemoryBalanceStore();
    }

    @Test
    void testDeposit() {
        long amount = FixedPoint.fromDouble(1000.0);
        store.deposit(USER_ID, ASSET_ID, amount);
        assertEquals(amount, store.getAvailable(USER_ID, ASSET_ID));
        assertEquals(0L, store.getLocked(USER_ID, ASSET_ID));
    }

    @Test
    void testHoldSuccess() {
        long deposit = FixedPoint.fromDouble(1000.0);
        long holdAmt = FixedPoint.fromDouble(500.0);

        store.deposit(USER_ID, ASSET_ID, deposit);
        boolean result = store.hold(USER_ID, ASSET_ID, holdAmt, ORDER_ID);

        assertTrue(result);
        assertEquals(FixedPoint.fromDouble(500.0), store.getAvailable(USER_ID, ASSET_ID));
        assertEquals(FixedPoint.fromDouble(500.0), store.getLocked(USER_ID, ASSET_ID));
    }

    @Test
    void testHoldInsufficientBalance() {
        long deposit = FixedPoint.fromDouble(100.0);
        long holdAmt = FixedPoint.fromDouble(500.0);

        store.deposit(USER_ID, ASSET_ID, deposit);
        boolean result = store.hold(USER_ID, ASSET_ID, holdAmt, ORDER_ID);

        assertFalse(result);
        // Balance unchanged
        assertEquals(deposit, store.getAvailable(USER_ID, ASSET_ID));
        assertEquals(0L, store.getLocked(USER_ID, ASSET_ID));
    }

    @Test
    void testRelease() {
        long deposit = FixedPoint.fromDouble(1000.0);
        long holdAmt = FixedPoint.fromDouble(500.0);

        store.deposit(USER_ID, ASSET_ID, deposit);
        store.hold(USER_ID, ASSET_ID, holdAmt, ORDER_ID);

        boolean result = store.release(USER_ID, ASSET_ID, holdAmt, ORDER_ID);
        assertTrue(result);
        assertEquals(deposit, store.getAvailable(USER_ID, ASSET_ID));
        assertEquals(0L, store.getLocked(USER_ID, ASSET_ID));
    }

    @Test
    void testSettle() {
        long buyerUser = 1L;
        long sellerUser = 2L;
        int baseAsset = 1; // BTC
        int quoteAsset = 0; // USD
        long baseAmount = FixedPoint.fromDouble(1.0);
        long quoteAmount = FixedPoint.fromDouble(50000.0);

        // Setup: buyer has quote locked, seller has base locked
        store.deposit(buyerUser, quoteAsset, quoteAmount);
        store.hold(buyerUser, quoteAsset, quoteAmount, 100L);

        store.deposit(sellerUser, baseAsset, baseAmount);
        store.hold(sellerUser, baseAsset, baseAmount, 200L);

        store.settle(buyerUser, sellerUser, baseAsset, quoteAsset, baseAmount, quoteAmount, 999L);

        // Buyer: locked quote gone, available base received
        assertEquals(0L, store.getLocked(buyerUser, quoteAsset));
        assertEquals(baseAmount, store.getAvailable(buyerUser, baseAsset));

        // Seller: locked base gone, available quote received
        assertEquals(0L, store.getLocked(sellerUser, baseAsset));
        assertEquals(quoteAmount, store.getAvailable(sellerUser, quoteAsset));
    }

    @Test
    void testSettleIdempotent() {
        long buyerUser = 1L;
        long sellerUser = 2L;
        int baseAsset = 1;
        int quoteAsset = 0;
        long baseAmount = FixedPoint.fromDouble(1.0);
        long quoteAmount = FixedPoint.fromDouble(50000.0);

        store.deposit(buyerUser, quoteAsset, quoteAmount * 2);
        store.hold(buyerUser, quoteAsset, quoteAmount, 100L);
        store.deposit(sellerUser, baseAsset, baseAmount * 2);
        store.hold(sellerUser, baseAsset, baseAmount, 200L);

        // First settle
        store.settle(buyerUser, sellerUser, baseAsset, quoteAsset, baseAmount, quoteAmount, 999L);
        long buyerBase = store.getAvailable(buyerUser, baseAsset);

        // Second settle with same tradeId — should be no-op
        store.settle(buyerUser, sellerUser, baseAsset, quoteAsset, baseAmount, quoteAmount, 999L);
        assertEquals(buyerBase, store.getAvailable(buyerUser, baseAsset));
    }

    @Test
    void testWithdraw() {
        long deposit = FixedPoint.fromDouble(1000.0);
        long withdrawAmt = FixedPoint.fromDouble(300.0);

        store.deposit(USER_ID, ASSET_ID, deposit);
        store.withdraw(USER_ID, ASSET_ID, withdrawAmt);

        assertEquals(FixedPoint.fromDouble(700.0), store.getAvailable(USER_ID, ASSET_ID));
    }

    @Test
    void testWithdrawInsufficientThrows() {
        store.deposit(USER_ID, ASSET_ID, FixedPoint.fromDouble(100.0));
        assertThrows(IllegalStateException.class,
                () -> store.withdraw(USER_ID, ASSET_ID, FixedPoint.fromDouble(200.0)));
    }

    @Test
    void testZeroBalanceOnNew() {
        assertEquals(0L, store.getAvailable(999L, 0));
        assertEquals(0L, store.getLocked(999L, 0));
    }
}
