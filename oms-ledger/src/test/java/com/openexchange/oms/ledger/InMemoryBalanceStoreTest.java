// SPDX-License-Identifier: Apache-2.0
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

        // First settle — newly applied → returns true
        boolean firstApplied = store.settle(buyerUser, sellerUser, baseAsset, quoteAsset, baseAmount, quoteAmount, 999L);
        assertTrue(firstApplied);
        long buyerBase = store.getAvailable(buyerUser, baseAsset);

        // Second settle with same tradeId — duplicate → returns false and is a no-op
        boolean secondApplied = store.settle(buyerUser, sellerUser, baseAsset, quoteAsset, baseAmount, quoteAmount, 999L);
        assertFalse(secondApplied);
        assertEquals(buyerBase, store.getAvailable(buyerUser, baseAsset));
    }

    /**
     * oms#84: an over-settle where the buyer's locked pool was under-held must clamp
     * locked to 0 (never negative), pull the shortfall from the buyer's available,
     * still credit the counterparty in full, return true, and bump the over-settle
     * counter exactly once.
     */
    @Test
    void testSettleOverSettleClampsLockedAndRecoversFromAvailable() {
        long buyerUser = 1L;
        long sellerUser = 2L;
        int baseAsset = 1; // BTC
        int quoteAsset = 0; // USD
        long baseAmount = FixedPoint.fromDouble(1.0);
        long quoteAmount = FixedPoint.fromDouble(50000.0); // trade consumes 50k quote from buyer
        long underHeld = FixedPoint.fromDouble(30000.0);   // but only 30k was ever locked

        // Buyer deposited the full 50k but only 30k is locked; 20k sits in available.
        store.deposit(buyerUser, quoteAsset, quoteAmount);
        store.hold(buyerUser, quoteAsset, underHeld, 100L);
        // Seller is normal: base fully held.
        store.deposit(sellerUser, baseAsset, baseAmount);
        store.hold(sellerUser, baseAsset, baseAmount, 200L);

        boolean applied = store.settle(buyerUser, sellerUser, baseAsset, quoteAsset, baseAmount, quoteAmount, 999L);
        assertTrue(applied, "settle must not reject an over-settle — the trade already happened");

        // Buyer locked quote clamps to 0, never negative.
        assertEquals(0L, store.getLocked(buyerUser, quoteAsset));
        // The 20k shortfall was pulled from buyer's available quote, flooring it at 0.
        assertEquals(0L, store.getAvailable(buyerUser, quoteAsset));
        // Counterparty crediting is unconditional: buyer gets the base in full.
        assertEquals(baseAmount, store.getAvailable(buyerUser, baseAsset));
        // Seller settles normally and is credited in full.
        assertEquals(0L, store.getLocked(sellerUser, baseAsset));
        assertEquals(quoteAmount, store.getAvailable(sellerUser, quoteAsset));

        // Exactly one over-settle event recorded.
        assertEquals(1L, store.getOversettleCount());
    }

    /**
     * oms#84: when the shortfall exceeds even the user's available, available floors
     * at 0, the remaining deficit is unrecovered (but still counted), and no balance
     * anywhere goes negative.
     */
    @Test
    void testSettleShortfallBeyondAvailableFloorsAtZero() {
        long buyerUser = 1L;
        long sellerUser = 2L;
        int baseAsset = 1;
        int quoteAsset = 0;
        long baseAmount = FixedPoint.fromDouble(1.0);
        long quoteAmount = FixedPoint.fromDouble(50000.0); // trade consumes 50k
        long underHeld = FixedPoint.fromDouble(30000.0);   // only 30k ever locked, nothing spare

        // Buyer deposited exactly 30k and locked all of it: no available cushion.
        store.deposit(buyerUser, quoteAsset, underHeld);
        store.hold(buyerUser, quoteAsset, underHeld, 100L);
        store.deposit(sellerUser, baseAsset, baseAmount);
        store.hold(sellerUser, baseAsset, baseAmount, 200L);

        boolean applied = store.settle(buyerUser, sellerUser, baseAsset, quoteAsset, baseAmount, quoteAmount, 999L);
        assertTrue(applied);

        // Locked clamps to 0 (not -20k) and available floors at 0 (not -20k).
        assertEquals(0L, store.getLocked(buyerUser, quoteAsset));
        assertEquals(0L, store.getAvailable(buyerUser, quoteAsset));
        // Counterparty still credited in full.
        assertEquals(baseAmount, store.getAvailable(buyerUser, baseAsset));
        assertEquals(quoteAmount, store.getAvailable(sellerUser, quoteAsset));
        assertEquals(0L, store.getLocked(sellerUser, baseAsset));

        // No balance anywhere is negative.
        assertTrue(store.getLocked(buyerUser, quoteAsset) >= 0);
        assertTrue(store.getAvailable(buyerUser, quoteAsset) >= 0);
        assertTrue(store.getLocked(sellerUser, baseAsset) >= 0);

        // The over-settle is still counted even though it could not be fully recovered.
        assertEquals(1L, store.getOversettleCount());
    }

    /**
     * oms#84 regression: a fully-held (normal) settle behaves exactly as before the
     * floor guard — balances identical and the over-settle counter stays at 0.
     */
    @Test
    void testSettleNormalDoesNotIncrementOversettleCounter() {
        long buyerUser = 1L;
        long sellerUser = 2L;
        int baseAsset = 1;
        int quoteAsset = 0;
        long baseAmount = FixedPoint.fromDouble(1.0);
        long quoteAmount = FixedPoint.fromDouble(50000.0);

        store.deposit(buyerUser, quoteAsset, quoteAmount);
        store.hold(buyerUser, quoteAsset, quoteAmount, 100L);
        store.deposit(sellerUser, baseAsset, baseAmount);
        store.hold(sellerUser, baseAsset, baseAmount, 200L);

        boolean applied = store.settle(buyerUser, sellerUser, baseAsset, quoteAsset, baseAmount, quoteAmount, 999L);
        assertTrue(applied);

        // Identical to legacy behavior.
        assertEquals(0L, store.getLocked(buyerUser, quoteAsset));
        assertEquals(baseAmount, store.getAvailable(buyerUser, baseAsset));
        assertEquals(0L, store.getLocked(sellerUser, baseAsset));
        assertEquals(quoteAmount, store.getAvailable(sellerUser, quoteAsset));
        // No spurious over-settle.
        assertEquals(0L, store.getOversettleCount());
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
