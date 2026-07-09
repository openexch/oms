// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.core;

import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.common.enums.OmsOrderType;
import com.openexchange.oms.common.enums.OrderSide;
import com.openexchange.oms.common.enums.TimeInForce;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.LongHashSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * oms#53: clusterOrderId-less orders in submitted states (ack lost at a seam,
 * fills matched by omsOrderId) must not be permanent zombies. Cluster-open
 * ones are re-linked to their clusterOrderId from the snapshot; cluster-gone
 * ones are terminalized and persisted like any other lost order.
 */
class OmsCoreEngineReconcileTest {

    private static final long NOW = 1_000_000_000L;
    private static final long OLD = NOW - 60_000;   // well past ORPHAN_MIN_AGE
    private static final long FRESH = NOW - 1_000;  // inside ORPHAN_MIN_AGE

    private OrderLifecycleManager lifecycle;
    private OmsCoreEngine engine;
    private List<OmsOrder> persisted;

    @BeforeEach
    void setUp() {
        lifecycle = new OrderLifecycleManager();
        engine = new OmsCoreEngine(lifecycle, new SyntheticOrderEngine());
        persisted = new ArrayList<>();
        engine.setPersistenceHandler(new OmsCoreEngine.PersistenceHandler() {
            @Override
            public void persistOrderUpdate(OmsOrder order) { persisted.add(order); }

            @Override
            public void persistExecution(com.openexchange.oms.common.domain.ExecutionReport report) { }
        });
    }

    private OmsOrder activeOrder(long omsId, OmsOrderStatus status, long clusterOrderId,
                                 long createdAtMs, long filledQty) {
        OmsOrder o = new OmsOrder();
        o.setOmsOrderId(omsId);
        o.setClusterOrderId(clusterOrderId);
        o.setUserId(7);
        o.setMarketId(1);
        o.setOrderType(OmsOrderType.LIMIT);
        o.setSide(OrderSide.BUY);
        o.setTimeInForce(TimeInForce.GTC);
        o.setPrice(100_00000000L);
        o.setQuantity(10_00000000L);
        o.setFilledQty(filledQty);
        o.setRemainingQty(10_00000000L - filledQty);
        o.setStatus(status);
        o.setCreatedAtMs(createdAtMs);
        lifecycle.restoreOrder(o);
        return o;
    }

    private int reconcile(LongHashSet clusterOpen, Long2LongHashMap omsToCluster) {
        return engine.reconcileAgainstOpenOrders(clusterOpen, omsToCluster, /*maxOrderId*/ 1_000, NOW);
    }

    @Test
    void zombiePartiallyFilledWithoutClusterIdIsTerminalized() {
        OmsOrder zombie = activeOrder(101, OmsOrderStatus.PARTIALLY_FILLED, 0, OLD, 3_00000000L);

        int repaired = reconcile(new LongHashSet(), new Long2LongHashMap(0L));

        assertEquals(1, repaired);
        assertEquals(OmsOrderStatus.CANCELLED, zombie.getStatus());
        assertTrue(persisted.contains(zombie), "repair must persist or it resurrects on rebuild");
    }

    @Test
    void zombieNewWithoutClusterIdIsTerminalized() {
        OmsOrder zombie = activeOrder(102, OmsOrderStatus.NEW, 0, OLD, 0);
        assertEquals(1, reconcile(new LongHashSet(), new Long2LongHashMap(0L)));
        assertEquals(OmsOrderStatus.CANCELLED, zombie.getStatus());
    }

    @Test
    void clusterOpenOrderWithoutClusterIdIsRelinkedNotTerminalized() {
        OmsOrder unlinked = activeOrder(103, OmsOrderStatus.PARTIALLY_FILLED, 0, OLD, 3_00000000L);

        LongHashSet clusterOpen = new LongHashSet();
        clusterOpen.add(555L);
        Long2LongHashMap omsToCluster = new Long2LongHashMap(0L);
        omsToCluster.put(103L, 555L);

        int repaired = reconcile(clusterOpen, omsToCluster);

        assertEquals(0, repaired, "still open on cluster: not a repair");
        assertEquals(OmsOrderStatus.PARTIALLY_FILLED, unlinked.getStatus());
        assertEquals(555L, unlinked.getClusterOrderId(), "clusterOrderId adopted from snapshot");
        assertSame(unlinked, lifecycle.getByClusterOrderId(555L), "egress correlation restored");
        assertTrue(persisted.contains(unlinked), "adopted link must be persisted");
    }

    @Test
    void freshOrPreClusterOrTriggerOrdersAreUntouched() {
        OmsOrder fresh = activeOrder(104, OmsOrderStatus.PENDING_NEW, 0, FRESH, 0);
        OmsOrder preCluster = activeOrder(105, OmsOrderStatus.PENDING_HOLD, 0, OLD, 0);
        OmsOrder trigger = activeOrder(106, OmsOrderStatus.PENDING_TRIGGER, 0, OLD, 0);

        assertEquals(0, reconcile(new LongHashSet(), new Long2LongHashMap(0L)));
        assertEquals(OmsOrderStatus.PENDING_NEW, fresh.getStatus());
        assertEquals(OmsOrderStatus.PENDING_HOLD, preCluster.getStatus());
        assertEquals(OmsOrderStatus.PENDING_TRIGGER, trigger.getStatus());
    }

    @Test
    void staleSubmittedOrphansAreDetectedForTheSweep() {
        // oms#41: the 1s sweep uses this to trigger a reconcile when in-flight
        // orders lost at a seam would otherwise sit PENDING_NEW forever.
        assertFalse(engine.hasStaleSubmittedOrphans(NOW), "empty book: no orphans");

        OmsOrder fresh = activeOrder(120, OmsOrderStatus.PENDING_NEW, 0, FRESH, 0);
        assertFalse(engine.hasStaleSubmittedOrphans(NOW), "inside the age gate: not stale yet");

        OmsOrder linked = activeOrder(121, OmsOrderStatus.NEW, 777, OLD, 0);
        OmsOrder preCluster = activeOrder(122, OmsOrderStatus.PENDING_HOLD, 0, OLD, 0);
        assertFalse(engine.hasStaleSubmittedOrphans(NOW),
                "linked and pre-cluster orders are not orphans");

        OmsOrder stale = activeOrder(123, OmsOrderStatus.PENDING_NEW, 0, OLD, 0);
        assertTrue(engine.hasStaleSubmittedOrphans(NOW), "aged clusterOrderId-less PENDING_NEW");
    }

    @Test
    void linkedOrderRepairStillWorksAndFullyFilledBecomesFilled() {
        OmsOrder gone = activeOrder(107, OmsOrderStatus.NEW, 555, OLD, 0);
        OmsOrder full = activeOrder(108, OmsOrderStatus.PARTIALLY_FILLED, 556, OLD, 10_00000000L);
        OmsOrder open = activeOrder(109, OmsOrderStatus.NEW, 557, OLD, 0);

        LongHashSet clusterOpen = new LongHashSet();
        clusterOpen.add(557L);

        assertEquals(2, reconcile(clusterOpen, new Long2LongHashMap(0L)));
        assertEquals(OmsOrderStatus.CANCELLED, gone.getStatus());
        assertEquals(OmsOrderStatus.FILLED, full.getStatus());
        assertEquals(OmsOrderStatus.NEW, open.getStatus());
    }

    // ==================== MONEY-A: defer release-bearing terminalize while a fill may be in flight ====

    @Test
    void partiallyFilledVanishedWithRecentFillIsDeferredThenTerminalizedWhenQuiet() {
        // MONEY-A: order 900 vanished from the cluster while a fill may still be in flight (recent
        // fill activity — this repair is often gap-triggered, so filledQty can be stale). Marking it
        // CANCELLED now would release a hold the cluster already consumed; the late fill's settlement
        // then double-debits `locked`. It must be DEFERRED, not terminalized, while fills are recent.
        OmsOrder o = activeOrder(150, OmsOrderStatus.PARTIALLY_FILLED, 900, OLD, 3_00000000L);
        o.setUpdatedAtMs(FRESH); // fill activity 1s ago — inside ORPHAN_MIN_AGE

        assertEquals(0, reconcile(new LongHashSet(), new Long2LongHashMap(0L)),
                "recent fill activity: the release-bearing terminalize must be deferred");
        assertEquals(OmsOrderStatus.PARTIALLY_FILLED, o.getStatus(),
                "not terminalized while a late fill may still be settling");
        assertFalse(persisted.contains(o), "nothing terminalized: nothing to persist");

        // Once fill activity has been quiet past the gate, a genuinely-cancelled order terminalizes.
        o.setUpdatedAtMs(OLD);
        assertEquals(1, reconcile(new LongHashSet(), new Long2LongHashMap(0L)));
        assertEquals(OmsOrderStatus.CANCELLED, o.getStatus());
        assertTrue(persisted.contains(o));
    }

    @Test
    void fullyFilledVanishedIsTerminalizedImmediatelyEvenWhenFillIsRecent() {
        // A fully-filled vanished order releases no hold (→ FILLED), so it is always safe to
        // terminalize now — the freshness gate applies only to the release-bearing (CANCELLED) case.
        OmsOrder o = activeOrder(151, OmsOrderStatus.PARTIALLY_FILLED, 901, OLD, 10_00000000L);
        o.setUpdatedAtMs(FRESH);

        assertEquals(1, reconcile(new LongHashSet(), new Long2LongHashMap(0L)));
        assertEquals(OmsOrderStatus.FILLED, o.getStatus());
    }

    // ==================== Cancel-and-replace repair (oms#67) ====================

    @Test
    void replacePendingResolvedFromSnapshotWhenNewLegEgressLost() {
        // Amend in flight: old leg cid=600 was cancelled on the cluster, NEW(601) egress lost.
        // The snapshot maps omsOrderId -> 601: the reconcile must RESOLVE (re-link + apply
        // pending values), never terminalize.
        OmsOrder order = activeOrder(140, OmsOrderStatus.NEW, 600, OLD, 0);
        assertTrue(lifecycle.onReplaceSubmitted(140, 120_00000000L, 10_00000000L, 0, 1_200L));

        LongHashSet clusterOpen = new LongHashSet();
        clusterOpen.add(601L);
        Long2LongHashMap omsToCluster = new Long2LongHashMap(0L);
        omsToCluster.put(140L, 601L);

        assertEquals(0, reconcile(clusterOpen, omsToCluster));
        assertEquals(OmsOrderStatus.NEW, order.getStatus());
        assertEquals(601L, order.getClusterOrderId());
        assertEquals(120_00000000L, order.getPrice());
        assertFalse(order.isReplacePending());
        assertSame(order, lifecycle.getByClusterOrderId(601L));
        assertTrue(persisted.contains(order), "resolved re-link must be persisted");
    }

    @Test
    void replacePendingStillOnOldLegIsLeftToTheTimeout() {
        // The snapshot still shows the OLD leg open: the amend simply has not applied yet.
        // Not a repair — the replace timeout owns the case where egress never comes.
        OmsOrder order = activeOrder(141, OmsOrderStatus.NEW, 700, OLD, 0);
        assertTrue(lifecycle.onReplaceSubmitted(141, 120_00000000L, 10_00000000L, 0, 1_200L));

        LongHashSet clusterOpen = new LongHashSet();
        clusterOpen.add(700L);
        Long2LongHashMap omsToCluster = new Long2LongHashMap(0L);
        omsToCluster.put(141L, 700L);

        assertEquals(0, reconcile(clusterOpen, omsToCluster));
        assertEquals(OmsOrderStatus.NEW, order.getStatus());
        assertEquals(700L, order.getClusterOrderId());
        assertTrue(order.isReplacePending(), "amend still in flight: marker stays");
    }

    @Test
    void replacePendingWithBothLegsGoneIsTerminalizedPastTheWindow() {
        OmsOrder order = activeOrder(142, OmsOrderStatus.NEW, 800, OLD, 0);
        assertTrue(lifecycle.onReplaceSubmitted(142, 120_00000000L, 10_00000000L, 0, 1_200L));
        // Simulate an old replace: requested well past the age gate.
        order.setReplaceRequestedAtMs(OLD);

        assertEquals(1, reconcile(new LongHashSet(), new Long2LongHashMap(0L)));
        assertEquals(OmsOrderStatus.CANCELLED, order.getStatus());
        assertFalse(order.isReplacePending(), "marker cleared before the terminal repair");
        assertTrue(persisted.contains(order));
    }
}
