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
}
