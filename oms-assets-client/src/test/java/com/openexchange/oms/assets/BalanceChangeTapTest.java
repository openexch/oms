// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the {@link BalanceChangeConsumer} change-tap on {@link AeronAssetsBalanceStore}: it fires the
 * projection's ABSOLUTE post-change {@code (available, locked)} on every absolute BalanceUpdate,
 * DepositAck, WithdrawAck, and read-your-hold delta application — and does nothing (no NPE, no call)
 * when no consumer is attached.
 */
class BalanceChangeTapTest {

    private static final int ASSET = 0;
    private static final long USER = 7L;

    /** Minimal fake: the store attaches itself as the egress listener; nothing here needs to ack. */
    private static final class FakeTransport implements AssetsTransport {
        @Override public boolean submitHold(long c, long o, long u, int a, long amt, boolean m) { return true; }
        @Override public boolean submitRelease(long orderId, long userId, long amount) { return true; }
        @Override public boolean submitDeposit(long c, long u, int a, long amt) { return true; }
        @Override public boolean submitWithdraw(long c, long u, int a, long amt) { return true; }
        @Override public boolean submitRequestBalanceSnapshot(long correlationId) { return true; }
        @Override public boolean submitRequestHoldSnapshot(long correlationId) { return true; }
        @Override public boolean isConnected() { return true; }
        @Override public void setEgressListener(AssetsEgressListener l) { /* store attaches itself */ }
    }

    /** {userId, assetId, available, locked} per tap invocation, in order. */
    private final List<long[]> taps = new ArrayList<>();
    private FakeTransport transport;
    private AeronAssetsBalanceStore store;

    @BeforeEach
    void setUp() {
        transport = new FakeTransport();
        store = new AeronAssetsBalanceStore(transport, 6, 50, 100);
        store.setBalanceChangeConsumer((userId, assetId, available, locked) ->
                taps.add(new long[] {userId, assetId, available, locked}));
    }

    private void assertTap(int index, long userId, int assetId, long available, long locked) {
        long[] t = taps.get(index);
        assertEquals(userId, t[0], "userId");
        assertEquals(assetId, t[1], "assetId");
        assertEquals(available, t[2], "available");
        assertEquals(locked, t[3], "locked");
    }

    @Test
    void absoluteBalanceUpdateForwardsBothValuesVerbatim() {
        store.onBalanceUpdate(USER, ASSET, 1_000L, 250L);
        assertEquals(1, taps.size());
        assertTap(0, USER, ASSET, 1_000L, 250L);
    }

    @Test
    void depositAckForwardsNewAvailableWithCurrentLocked() {
        store.onBalanceUpdate(USER, ASSET, 1_000L, 300L); // seed locked=300
        store.onDepositAck(0L, USER, ASSET, 500L, 1_500L);
        assertEquals(2, taps.size());
        // Deposit never touches locked: available=newAvailable, locked=the projection's current 300.
        assertTap(1, USER, ASSET, 1_500L, 300L);
    }

    @Test
    void withdrawAckForwardsNewAvailableWithCurrentLocked() {
        store.onBalanceUpdate(USER, ASSET, 1_000L, 300L); // seed locked=300
        store.onWithdrawAck(0L, USER, ASSET, 400L, 600L);
        assertEquals(2, taps.size());
        assertTap(1, USER, ASSET, 600L, 300L);
    }

    @Test
    void holdAckForwardsProjectionAbsolutesAfterApplyingTheDelta() {
        store.onBalanceUpdate(USER, ASSET, 1_000L, 0L); // seed avail=1000 locked=0
        store.onHoldAck(0L, 42L, USER, ASSET, 400L);    // delta: -400 avail, +400 locked
        assertEquals(2, taps.size());
        // Read-your-hold: absolutes AFTER applying the delta.
        assertTap(1, USER, ASSET, 600L, 400L);
        // The projection itself agrees (sanity: the tap mirrors the projection, not the raw delta).
        assertEquals(600L, store.getAvailable(USER, ASSET));
        assertEquals(400L, store.getLocked(USER, ASSET));
    }

    @Test
    void noConsumerAttachedIsANoOpAndNeverThrows() {
        store.setBalanceChangeConsumer(null);
        store.onBalanceUpdate(USER, ASSET, 1_000L, 250L);
        store.onDepositAck(0L, USER, ASSET, 500L, 1_500L);
        store.onWithdrawAck(0L, USER, ASSET, 100L, 1_400L);
        store.onHoldAck(0L, 42L, USER, ASSET, 200L);
        assertTrue(taps.isEmpty(), "no consumer attached => zero tap invocations");
        // The projection still tracks correctly without a tap: 1000 +500 dep -100 wd -200 hold = 1200
        // available; locked 250 (seed) +200 (hold) = 450.
        assertEquals(1_200L, store.getAvailable(USER, ASSET));
        assertEquals(450L, store.getLocked(USER, ASSET));
    }

    @Test
    void detachingTheConsumerStopsFurtherTaps() {
        store.onBalanceUpdate(USER, ASSET, 1_000L, 0L);
        assertEquals(1, taps.size());
        store.setBalanceChangeConsumer(null);
        store.onBalanceUpdate(USER, ASSET, 2_000L, 0L);
        assertEquals(1, taps.size(), "no further taps after detach");
    }
}
