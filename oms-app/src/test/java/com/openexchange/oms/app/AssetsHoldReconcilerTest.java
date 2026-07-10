// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.openexchange.oms.assets.AeronAssetsBalanceStore;
import com.openexchange.oms.assets.AssetsEgressListener;
import com.openexchange.oms.assets.AssetsTransport;
import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.domain.SnowflakeIdGenerator;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.common.enums.OmsOrderType;
import com.openexchange.oms.common.enums.OrderSide;
import com.openexchange.oms.core.OrderLifecycleManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the money-critical orphan-hold contract: the reconciler releases ONLY a provably-never-submitted
 * hold (2a unknown / 2b known-pre-cluster / 2c terminal-never-reached-cluster), gated on the 60s age
 * gate AND two consecutive sweeps, and SURFACES (never sweeps) anything that reached the cluster or is
 * otherwise unprovable. No cluster boot: a real {@link AeronAssetsBalanceStore} over a scripted fake
 * transport, a real {@link OrderLifecycleManager}, a fake PG lookup, and an adjustable clock.
 */
class AssetsHoldReconcilerTest {

    private static final int ASSET = 0;
    private static final long USER = 7L;

    /** Adjustable clock so age-gate branches are deterministic. */
    private static final class TestClock extends Clock {
        private volatile long millis;

        TestClock(long millis) {
            this.millis = millis;
        }

        void set(long millis) {
            this.millis = millis;
        }

        @Override public long millis() {
            return millis;
        }

        @Override public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    /** Records releases and snapshot-request correlationIds; everything else is a harmless success. */
    private static final class FakeTransport implements AssetsTransport {
        final List<long[]> releases = new CopyOnWriteArrayList<>(); // {orderId, userId, amount}
        volatile long lastBalanceSnapCorr;
        volatile long lastHoldSnapCorr;
        volatile boolean holdSnapAcceptsRequests = true;
        AssetsEgressListener listener;

        @Override public boolean submitHold(long c, long o, long u, int a, long amt, boolean m) {
            return true;
        }

        @Override public boolean submitRelease(long orderId, long userId, long amount) {
            releases.add(new long[] {orderId, userId, amount});
            return true;
        }

        @Override public boolean submitDeposit(long c, long u, int a, long amt) {
            return true;
        }

        @Override public boolean submitWithdraw(long c, long u, int a, long amt) {
            return true;
        }

        @Override public boolean submitRequestBalanceSnapshot(long correlationId) {
            lastBalanceSnapCorr = correlationId;
            return true;
        }

        @Override public boolean submitRequestHoldSnapshot(long correlationId) {
            lastHoldSnapCorr = correlationId;
            return holdSnapAcceptsRequests;
        }

        @Override public boolean isConnected() {
            return true;
        }

        @Override public void setEgressListener(AssetsEgressListener l) {
            this.listener = l;
        }
    }

    private TestClock clock;
    private FakeTransport transport;
    private AeronAssetsBalanceStore store;
    private OrderLifecycleManager lifecycle;
    private Map<Long, OmsOrder> pg;
    private AssetsHoldReconciler reconciler;
    private long baseMs; // "creation" wall-clock the ids/orders are anchored to

    @BeforeEach
    void setUp() {
        baseMs = System.currentTimeMillis();
        clock = new TestClock(baseMs);
        transport = new FakeTransport();
        store = new AeronAssetsBalanceStore(transport, 6, 50, 100);
        lifecycle = new OrderLifecycleManager();
        pg = new HashMap<>();
        reconciler = new AssetsHoldReconciler(store, lifecycle, pg::get, clock);
    }

    // ---- helpers ----

    /** A Snowflake-like id whose decoded timestamp is exactly {@code tsMs}. Mirrors the generator layout. */
    private static long idAtMs(long tsMs) {
        // Inverse of the generator layout: ((ts - EPOCH) << 22) | node | seq ; EPOCH = 2024-01-01.
        final long epoch = 1704067200000L;
        long id = (tsMs - epoch) << 22;
        if (SnowflakeIdGenerator.timestampMillis(id) != tsMs) {
            throw new IllegalStateException("id layout mismatch");
        }
        return id;
    }

    private OmsOrder order(long omsOrderId, OmsOrderStatus status, long clusterOrderId, long createdAtMs) {
        OmsOrder o = new OmsOrder();
        o.setOmsOrderId(omsOrderId);
        o.setUserId(USER);
        o.setSide(OrderSide.BUY);
        o.setOrderType(OmsOrderType.LIMIT);
        o.setStatus(status);
        o.setClusterOrderId(clusterOrderId);
        o.setCreatedAtMs(createdAtMs);
        return o;
    }

    private AssetsHoldReconciler.HoldEntry hold(long orderId) {
        return new AssetsHoldReconciler.HoldEntry(orderId, USER, ASSET, 400L);
    }

    private AssetsHoldReconciler.Kind classifyNow(long orderId) {
        return reconciler.classify(hold(orderId), clock.millis()).kind();
    }

    // ==================== predicate: every branch ====================

    @Test
    void activeOrderIsLegitAndNeverAnOrphan() {
        long id = idAtMs(baseMs);
        OmsOrder o = order(id, OmsOrderStatus.PENDING_RISK, 0, baseMs);
        lifecycle.registerOrder(o); // enters as active PENDING_RISK
        clock.set(baseMs + 5 * 60_000); // even long after: an active order still holds legitimately
        assertEquals(AssetsHoldReconciler.Kind.ACTIVE_LEGIT, classifyNow(id));
    }

    @Test
    void activeOrderWithClusterOrderIdWinsOverReachedClusterSurface() {
        // Condition 1 is checked first: a live NEW order (with a cid) is LEGIT, not surfaced.
        long id = idAtMs(baseMs);
        OmsOrder o = order(id, OmsOrderStatus.PENDING_RISK, 0, baseMs);
        lifecycle.registerOrder(o);
        lifecycle.onRiskPassed(id);
        lifecycle.onHoldPlaced(id);
        lifecycle.onSentToCluster(id, 555L); // NEW-equivalent, cid assigned, still active
        assertEquals(AssetsHoldReconciler.Kind.ACTIVE_LEGIT, classifyNow(id));
    }

    @Test
    void unknownOrderYoungIsPending() {
        long id = idAtMs(baseMs);
        clock.set(baseMs + 5_000); // 5s old < 60s
        assertEquals(AssetsHoldReconciler.Kind.PENDING, classifyNow(id));
    }

    @Test
    void unknownOrderPastAgeGateIsEligible() {
        long id = idAtMs(baseMs);
        clock.set(baseMs + 120_000); // 120s old
        assertEquals(AssetsHoldReconciler.Kind.ELIGIBLE, classifyNow(id)); // 2a
    }

    @Test
    void knownPreClusterAgedIsEligible_2b() {
        long id = idAtMs(baseMs);
        pg.put(id, order(id, OmsOrderStatus.PENDING_NEW, 0, baseMs));
        clock.set(baseMs + 120_000);
        assertEquals(AssetsHoldReconciler.Kind.ELIGIBLE, classifyNow(id)); // 2b
    }

    @Test
    void knownPreClusterYoungIsPending() {
        long id = idAtMs(baseMs);
        pg.put(id, order(id, OmsOrderStatus.PENDING_HOLD, 0, baseMs));
        clock.set(baseMs + 10_000); // inside the submit-path window
        assertEquals(AssetsHoldReconciler.Kind.PENDING, classifyNow(id));
    }

    @Test
    void knownWithClusterOrderIdNonTerminalIsSurfaced() {
        long id = idAtMs(baseMs);
        pg.put(id, order(id, OmsOrderStatus.NEW, 999L, baseMs)); // reached the cluster
        clock.set(baseMs + 120_000);
        assertEquals(AssetsHoldReconciler.Kind.SURFACE, classifyNow(id));
    }

    @Test
    void terminalWithClusterOrderIdIsSurfacedNotReleased() {
        long id = idAtMs(baseMs);
        pg.put(id, order(id, OmsOrderStatus.CANCELLED, 999L, baseMs)); // terminal but DID reach cluster
        clock.set(baseMs + 120_000);
        assertEquals(AssetsHoldReconciler.Kind.SURFACE, classifyNow(id));
    }

    @Test
    void terminalWithoutClusterOrderIdAgedIsEligible_2c() {
        long id = idAtMs(baseMs);
        pg.put(id, order(id, OmsOrderStatus.REJECTED, 0, baseMs)); // pre-cluster reject
        clock.set(baseMs + 120_000);
        assertEquals(AssetsHoldReconciler.Kind.ELIGIBLE, classifyNow(id)); // 2c
    }

    @Test
    void knownPostClusterStatusWithoutClusterOrderIdIsSurfaced() {
        // NEW/PARTIALLY_FILLED with cid==0 = ack-lost zombie; may be resting on the cluster => surface.
        long id = idAtMs(baseMs);
        pg.put(id, order(id, OmsOrderStatus.PARTIALLY_FILLED, 0, baseMs));
        clock.set(baseMs + 120_000);
        assertEquals(AssetsHoldReconciler.Kind.SURFACE, classifyNow(id));
    }

    // ==================== two-sweep gate + release-through-store ====================

    @Test
    void eligibleOrphanReleasesOnlyOnTheSecondSweep() {
        long id = idAtMs(baseMs);
        clock.set(baseMs + 120_000);
        List<AssetsHoldReconciler.HoldEntry> snap = List.of(hold(id));

        reconciler.processSnapshot(snap);                  // 1st observation -> candidate, no release
        assertEquals(0, transport.releases.size());
        assertEquals(0, reconciler.getOrphanReleasesTotal());
        assertEquals(1, reconciler.getUnresolvedOrphansLastSweep());

        reconciler.processSnapshot(snap);                  // 2nd observation -> RELEASE
        assertEquals(1, transport.releases.size());
        long[] r = transport.releases.get(0);
        assertEquals(id, r[0]);
        assertEquals(USER, r[1]);
        assertEquals(-1L, r[2]);                           // full-residual sentinel via releaseAll
        assertEquals(1, reconciler.getOrphanReleasesTotal());
        assertEquals(0, reconciler.getUnresolvedOrphansLastSweep());
    }

    @Test
    void candidateThatDisappearsFromTheSecondSnapshotIsDroppedNotReleased() {
        long id = idAtMs(baseMs);
        clock.set(baseMs + 120_000);

        reconciler.processSnapshot(List.of(hold(id)));     // candidate
        assertEquals(1, reconciler.getUnresolvedOrphansLastSweep());

        reconciler.processSnapshot(List.of());             // gone from snapshot 2 -> dropped
        assertEquals(0, transport.releases.size());
        assertEquals(0, reconciler.getOrphanReleasesTotal());
        assertEquals(0, reconciler.getUnresolvedOrphansLastSweep());

        // Re-appearing now restarts the two-sweep clock (it was dropped, not confirmed).
        reconciler.processSnapshot(List.of(hold(id)));     // candidate again, still no release
        assertEquals(0, transport.releases.size());
        assertEquals(1, reconciler.getUnresolvedOrphansLastSweep());
    }

    @Test
    void youngUnknownIsHeldAcrossSweepsThenReleasesOnceAged() {
        long id = idAtMs(baseMs);
        clock.set(baseMs + 5_000); // young
        List<AssetsHoldReconciler.HoldEntry> snap = List.of(hold(id));

        reconciler.processSnapshot(snap); // PENDING (age) -> not a candidate
        reconciler.processSnapshot(snap); // still young -> still not eligible, never released
        assertEquals(0, transport.releases.size());
        assertEquals(1, reconciler.getUnresolvedOrphansLastSweep());

        clock.set(baseMs + 120_000);      // now aged
        reconciler.processSnapshot(snap); // 1st ELIGIBLE observation -> candidate
        assertEquals(0, transport.releases.size());
        reconciler.processSnapshot(snap); // 2nd ELIGIBLE observation -> RELEASE
        assertEquals(1, transport.releases.size());
        assertEquals(1, reconciler.getOrphanReleasesTotal());
    }

    @Test
    void activeOrderHoldIsNeverReleasedAndNeverUnresolved() {
        long id = idAtMs(baseMs);
        lifecycle.registerOrder(order(id, OmsOrderStatus.PENDING_RISK, 0, baseMs));
        clock.set(baseMs + 120_000);
        List<AssetsHoldReconciler.HoldEntry> snap = List.of(hold(id));

        reconciler.processSnapshot(snap);
        reconciler.processSnapshot(snap);
        assertEquals(0, transport.releases.size());
        assertEquals(0, reconciler.getOrphanReleasesTotal());
        assertEquals(0, reconciler.getUnresolvedOrphansLastSweep()); // live order is excluded, not counted
    }

    @Test
    void reachedClusterHoldIsSurfacedEverySweepAndNeverReleased() {
        long id = idAtMs(baseMs);
        pg.put(id, order(id, OmsOrderStatus.CANCELLED, 999L, baseMs));
        clock.set(baseMs + 5 * 60_000);
        List<AssetsHoldReconciler.HoldEntry> snap = List.of(hold(id));

        for (int i = 0; i < 4; i++) {
            reconciler.processSnapshot(snap);
        }
        assertEquals(0, transport.releases.size());
        assertEquals(0, reconciler.getOrphanReleasesTotal());
        assertEquals(1, reconciler.getUnresolvedOrphansLastSweep()); // surfaced for a human, persistently
    }

    // ==================== end-to-end wiring: forwarding seam + projection gate ====================

    @Test
    void endToEndSweepForwardsSnapshotAndReleasesThroughTheStore() throws Exception {
        reconciler.start();                    // attaches the store's hold-snapshot forwarding seam
        try {
            makeProjectionReady();
            long id = idAtMs(baseMs);
            clock.set(baseMs + 120_000);

            driveSweep(id);                    // 1st sweep -> candidate
            awaitUnresolved(1);
            assertEquals(0, reconciler.getOrphanReleasesTotal());

            driveSweep(id);                    // 2nd sweep -> release via store.releaseAll
            awaitReleases(1);
            long[] r = transport.releases.get(0);
            assertEquals(id, r[0]);
            assertEquals(-1L, r[2]);
        } finally {
            reconciler.stop();
        }
    }

    @Test
    void sweepIsSkippedWhenAeProjectionNotReady() {
        reconciler.start();
        try {
            // projection NOT made ready
            reconciler.sweep();
            assertEquals(0L, transport.lastHoldSnapCorr); // no snapshot request issued
            assertEquals(1L, reconciler.getSweepsTotal()); // the attempt is still counted
        } finally {
            reconciler.stop();
        }
    }

    private void makeProjectionReady() {
        store.onConnected();                        // issues a balance-snapshot request
        store.onBalanceSnapshotEnd(transport.lastBalanceSnapCorr, 0);
        assertTrue(store.isProjectionReady());
    }

    /** Runs a manual sweep and hands the AE's one-entry snapshot answer back through the store. */
    private void driveSweep(long orderId) {
        reconciler.sweep();                         // -> store.requestHoldSnapshot -> fake records corr
        long corr = transport.lastHoldSnapCorr;
        store.onHoldSnapshotEntry(orderId, USER, ASSET, 400L); // forwarded to the reconciler consumer
        store.onHoldSnapshotEnd(corr, 1);           // -> reconciler hands processing to its scheduler
    }

    private void awaitUnresolved(long expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < deadline) {
            if (reconciler.getUnresolvedOrphansLastSweep() == expected) {
                return;
            }
            Thread.sleep(5);
        }
        assertEquals(expected, reconciler.getUnresolvedOrphansLastSweep());
    }

    private void awaitReleases(long expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < deadline) {
            if (reconciler.getOrphanReleasesTotal() == expected) {
                return;
            }
            Thread.sleep(5);
        }
        assertEquals(expected, reconciler.getOrphanReleasesTotal());
    }

    @Test
    void snowflakeTimestampRoundTrips() {
        SnowflakeIdGenerator gen = new SnowflakeIdGenerator(3);
        long before = System.currentTimeMillis();
        long id = gen.nextId();
        long after = System.currentTimeMillis();
        long ts = SnowflakeIdGenerator.timestampMillis(id);
        assertTrue(ts >= before && ts <= after, "decoded ts " + ts + " not in [" + before + "," + after + "]");
        // A synthetic id decodes exactly.
        assertEquals(1704067200000L + 90_000, SnowflakeIdGenerator.timestampMillis(idAtMs(1704067200000L + 90_000)));
    }
}
