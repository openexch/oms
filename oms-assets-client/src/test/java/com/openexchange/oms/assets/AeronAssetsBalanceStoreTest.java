// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the money-critical store semantics against a scripted fake transport: the timeout
 * compensator (exactly one, ordered AFTER the hold, suppressed for amend deltas), fail-closed
 * behavior, read-your-hold projection consistency, the settle high-water dedupe, and the
 * disconnect/reconnect compensation path. These rules are what make a wedged or restarting AE
 * unable to orphan a lock or double-apply a side effect.
 */
class AeronAssetsBalanceStoreTest {

    /** Records every submit in order; ack behavior scripted per test by driving the listener. */
    private static final class FakeTransport implements AssetsTransport {
        record Submitted(String type, long correlationId, long orderId, long userId, long amount,
                         boolean omsManagedRelease) {
        }

        final List<Submitted> submissions = new ArrayList<>();
        AssetsEgressListener listener;
        boolean connected = true;
        boolean acceptSubmits = true;
        /** When set, a Hold submit is immediately acked by the listener (synchronously). */
        boolean autoAckHolds = false;
        boolean autoRejectHolds = false;

        @Override
        public synchronized boolean submitHold(long corr, long orderId, long userId, int assetId,
                                               long amount, boolean omsManagedRelease) {
            if (!acceptSubmits) {
                return false;
            }
            submissions.add(new Submitted("HOLD", corr, orderId, userId, amount, omsManagedRelease));
            if (autoAckHolds) {
                listener.onHoldAck(corr, orderId, userId, assetId, amount);
            } else if (autoRejectHolds) {
                listener.onHoldReject(corr, orderId, userId, assetId, amount, 1);
            }
            return true;
        }

        @Override
        public synchronized boolean submitRelease(long orderId, long userId, long amount) {
            if (!acceptSubmits) {
                return false;
            }
            submissions.add(new Submitted("RELEASE", 0, orderId, userId, amount, false));
            return true;
        }

        @Override
        public synchronized boolean submitDeposit(long corr, long userId, int assetId, long amount) {
            submissions.add(new Submitted("DEPOSIT", corr, 0, userId, amount, false));
            return true;
        }

        @Override
        public synchronized boolean submitWithdraw(long corr, long userId, int assetId, long amount) {
            submissions.add(new Submitted("WITHDRAW", corr, 0, userId, amount, false));
            return true;
        }

        @Override
        public synchronized boolean submitRequestBalanceSnapshot(long corr) {
            submissions.add(new Submitted("BALSNAP", corr, 0, 0, 0, false));
            return true;
        }

        @Override
        public synchronized boolean submitRequestHoldSnapshot(long corr) {
            submissions.add(new Submitted("HOLDSNAP", corr, 0, 0, 0, false));
            return true;
        }

        @Override
        public boolean isConnected() {
            return connected;
        }

        @Override
        public void setEgressListener(AssetsEgressListener listener) {
            this.listener = listener;
        }

        synchronized Submitted last() {
            return submissions.get(submissions.size() - 1);
        }
    }

    private FakeTransport transport;
    private AeronAssetsBalanceStore store;

    @BeforeEach
    void setUp() {
        transport = new FakeTransport();
        store = new AeronAssetsBalanceStore(transport, 6, 50, 100); // short timeouts for tests
    }

    @Test
    void holdAckSucceedsAndProjectionShowsTheHoldImmediately() {
        transport.autoAckHolds = true;
        transport.listener.onBalanceUpdate(7L, 0, 1_000L, 0L); // seed
        assertTrue(store.hold(7L, 0, 400L, 42L));
        // Read-your-hold: the ack path applied the delta before the caller was released.
        assertEquals(600L, store.getAvailable(7L, 0));
        assertEquals(400L, store.getLocked(7L, 0));
        assertEquals(0L, store.getHoldTimeouts());
    }

    @Test
    void holdRejectFailsWithoutCompensator() {
        transport.autoRejectHolds = true;
        assertFalse(store.hold(7L, 0, 400L, 42L));
        assertEquals(1, transport.submissions.size()); // just the hold; no release
    }

    @Test
    void holdTimeoutSendsExactlyOneCompensatorOrderedAfterTheHold() {
        // No auto-ack: the future times out (50ms).
        assertFalse(store.hold(7L, 0, 400L, 42L));
        assertEquals(2, transport.submissions.size());
        assertEquals("HOLD", transport.submissions.get(0).type());
        FakeTransport.Submitted comp = transport.submissions.get(1);
        assertEquals("RELEASE", comp.type());
        assertEquals(42L, comp.orderId());
        assertEquals(7L, comp.userId());
        assertEquals(-1L, comp.amount());
        assertEquals(1L, store.getHoldTimeouts());
        assertEquals(1L, store.getCompensatorsSent());
    }

    @Test
    void amendDeltaTimeoutSuppressesTheCompensator() {
        // First: a successful create-hold so the orderId is known-acked.
        transport.autoAckHolds = true;
        assertTrue(store.hold(7L, 0, 400L, 42L));
        // Then an amend delta (same orderId) that times out.
        transport.autoAckHolds = false;
        assertFalse(store.hold(7L, 0, 100L, 42L));
        // Two HOLD submissions, ZERO releases: the -1 compensator would nuke the base hold.
        assertEquals(2, transport.submissions.stream().filter(s -> s.type().equals("HOLD")).count());
        assertEquals(0, transport.submissions.stream().filter(s -> s.type().equals("RELEASE")).count());
        assertEquals(1L, store.getAmendOrphans());
    }

    @Test
    void syntheticParentHoldCarriesTheOmsManagedFlag() {
        transport.autoAckHolds = true;
        assertTrue(store.hold(7L, 0, 400L, 42L, true));
        assertTrue(transport.submissions.get(0).omsManagedRelease());
    }

    @Test
    void disconnectedHoldFailsClosedWithoutSubmitting() {
        transport.connected = false;
        assertFalse(store.hold(7L, 0, 400L, 42L));
        assertEquals(0, transport.submissions.size());
    }

    @Test
    void settleDedupesOnHighWaterAcrossBootInit() {
        store.initSettleHighWater(100L);
        assertFalse(store.settle(1, 2, 1, 0, 10, 10, 100L)); // already applied pre-restart
        assertTrue(store.settle(1, 2, 1, 0, 10, 10, 101L));
        assertFalse(store.settle(1, 2, 1, 0, 10, 10, 101L)); // failover redelivery
        assertTrue(store.settle(1, 2, 1, 0, 10, 10, 102L));
        assertEquals(102L, store.getSettleHighWater());
    }

    @Test
    void releaseAllSendsTheFullResidualSentinelAndSupportsResidualHolds() {
        assertTrue(store.supportsResidualHolds());
        assertTrue(store.releaseAll(7L, 0, 42L));
        assertEquals(-1L, transport.last().amount());
    }

    @Test
    void withdrawRejectThrowsIllegalStateAndTimeoutSaysOutcomeUnknown() {
        // Reject path: complete the future from the listener on another thread after submit.
        Thread rejector = new Thread(() -> {
            awaitSubmission("WITHDRAW");
            FakeTransport.Submitted w = transport.last();
            transport.listener.onWithdrawReject(w.correlationId(), w.userId(), 0, w.amount(), 1);
        });
        rejector.start();
        assertThrows(IllegalStateException.class, () -> store.withdraw(7L, 0, 100L));

        // Timeout path: nobody acks -> "OUTCOME UNKNOWN" IllegalStateException.
        IllegalStateException unknown =
                assertThrows(IllegalStateException.class, () -> store.withdraw(7L, 0, 100L));
        assertTrue(unknown.getMessage().contains("OUTCOME UNKNOWN"));
    }

    @Test
    void depositAckUpdatesProjectionAvailable() {
        Thread acker = new Thread(() -> {
            awaitSubmission("DEPOSIT");
            FakeTransport.Submitted d = transport.last();
            transport.listener.onDepositAck(d.correlationId(), d.userId(), 0, d.amount(), 500L);
        });
        acker.start();
        store.deposit(7L, 0, 500L);
        assertEquals(500L, store.getAvailable(7L, 0));
    }

    @Test
    void disconnectFailsPendingHoldAndCompensatesOnReconnect() throws Exception {
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicBoolean result = new AtomicBoolean(true);
        Thread holder = new Thread(() -> {
            result.set(store.hold(7L, 0, 400L, 42L));
            done.countDown();
        });
        holder.start();
        awaitSubmission("HOLD");
        transport.listener.onDisconnected();       // session dies with the hold outcome unknown
        assertTrue(done.await(2, TimeUnit.SECONDS));
        assertFalse(result.get());                  // fail-closed

        transport.listener.onReconnected();         // compensator lands after reconnect
        assertTrue(transport.submissions.stream().anyMatch(
                s -> s.type().equals("RELEASE") && s.orderId() == 42L && s.amount() == -1L));
        // And the projection is marked not-ready until the snapshot completes.
        assertFalse(store.isProjectionReady());
        transport.listener.onBalanceSnapshotEnd(transport.last().type().equals("BALSNAP")
                ? transport.last().correlationId()
                : transport.submissions.stream().filter(s -> s.type().equals("BALSNAP"))
                        .reduce((a, b) -> b).orElseThrow().correlationId(), 0);
        assertTrue(store.isProjectionReady());
    }

    @Test
    void absoluteBalanceUpdateOverwritesProjectionSkew() {
        transport.listener.onBalanceUpdate(7L, 1, 111L, 22L);
        assertEquals(111L, store.getAvailable(7L, 1));
        assertEquals(22L, store.getLocked(7L, 1));
        transport.listener.onBalanceUpdate(7L, 1, 999L, 0L); // absolute overwrite self-corrects
        assertEquals(999L, store.getAvailable(7L, 1));
        assertEquals(0L, store.getLocked(7L, 1));
    }

    private void awaitSubmission(final String type) {
        final long deadline = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < deadline) {
            synchronized (transport) {
                if (transport.submissions.stream().anyMatch(s -> s.type().equals(type))) {
                    return;
                }
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("no " + type + " submitted within 2s");
    }
}
