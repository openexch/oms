// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.openexchange.oms.assets.AeronAssetsBalanceStore;
import com.openexchange.oms.assets.AssetsTransport;
import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.*;
import com.openexchange.oms.core.*;
import com.openexchange.oms.risk.*;
import org.agrona.collections.LongHashSet;
import org.agrona.collections.Long2LongHashMap;
import org.junit.jupiter.api.Test;
import java.time.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ProjectionSafetyTest {
    private static final long NOW = 1790611200000L;
    private OmsOrder order(long id, long cid) {
        var o = new OmsOrder();
        o.setOmsOrderId(id); o.setClusterOrderId(cid); o.setUserId(7); o.setMarketId(1);
        o.setClientOrderId("same-client-id"); o.setOrderType(OmsOrderType.LIMIT);
        o.setSide(OrderSide.BUY); o.setTimeInForce(TimeInForce.GTC);
        o.setStatus(OmsOrderStatus.NEW); o.setQuantity(100); o.setRemainingQty(100);
        o.setCreatedAtMs(NOW - 120000); o.setUpdatedAtMs(NOW - 120000);
        return o;
    }

    @Test void absentOrderIsNotCancelledEvenAfterAQuietPeriod() {
        for (long cid : new long[]{0, 42}) {
            var lifecycle = new OrderLifecycleManager();
            var core = new OmsCoreEngine(lifecycle, new SyntheticOrderEngine());
            var o = order(101, cid); lifecycle.restoreOrder(o);
            core.reconcileAgainstOpenOrders(new LongHashSet(), new Long2LongHashMap(0L), 1000, NOW);
            assertSame(o, lifecycle.getOrder(101));
            assertEquals(OmsOrderStatus.NEW, o.getStatus(), "absence cannot distinguish fill from cancel");
        }
    }

    @Test void repeatedAbsenceCannotAuthorizeHoldRelease() {
        var transport = mock(AssetsTransport.class);
        var store = new AeronAssetsBalanceStore(transport, 6, 50, 100);
        long id = (NOW - 120000 - 1704067200000L) << 22;
        var reconciler = new AssetsHoldReconciler(store, new OrderLifecycleManager(), ignored -> null,
                Clock.fixed(Instant.ofEpochMilli(NOW), ZoneOffset.UTC), ignored -> false);
        var snapshot = List.of(new AssetsHoldReconciler.HoldEntry(id, 7, 0, 400));
        for (int i = 0; i < 3; i++) reconciler.processSnapshot(snapshot);
        verify(transport, never()).submitRelease(anyLong(), anyLong(), anyLong());
        reconciler.stop();
    }

    @Test void clientIdClaimDoesNotLeaveAnUnindexedSecondOrder() {
        var lifecycle = new OrderLifecycleManager();
        // Valid interleaving: both callers observe absence before either registers.
        assertEquals(0, lifecycle.findActiveByClientOrderId(7, "same-client-id"));
        assertEquals(0, lifecycle.findActiveByClientOrderId(7, "same-client-id"));
        lifecycle.registerOrder(order(1, 0));
        lifecycle.registerOrder(order(2, 0));
        assertEquals(1, lifecycle.getActiveOrderCount());
        assertNull(lifecycle.getOrder(2));
    }

    @Test void corruptStoredRiskPreventsSuccessfulBootstrap() {
        var market = mock(MarketDataProvider.class);
        var risk = new RiskEngine(6, market, (u, a, v) -> true);
        var config = new RiskConfigManager(risk, 6);
        config.setConfig(1, RiskConfig.builder().maxOpenOrders(500).build());
        assertThrows(IllegalStateException.class, () -> RiskConfigBootstrap.replay(Map.of(1,
                new RiskConfigStore.StoredRow(Map.of("maxOpenOrders", "broken"), false)), config, risk));
    }

    @Test void replaceTimeoutPreservesAmbiguousReservation() {
        var lifecycle = new OrderLifecycleManager();
        var core = new OmsCoreEngine(lifecycle, new SyntheticOrderEngine());
        var o = order(101, 42); lifecycle.restoreOrder(o);
        assertTrue(lifecycle.onReplaceSubmitted(101, 200, 100, 50, 200));
        o.setReplaceRequestedAtMs(NOW - 120000);
        var hooks = mock(OrderLifecycleManager.ReplaceHooks.class); lifecycle.setReplaceHooks(hooks);
        core.checkGtdExpiry(NOW);
        assertTrue(o.isReplacePending(), "timeout does not prove the ME rejected the replace");
        verify(hooks, never()).onReplaceAborted(any());
    }

    @Test void gtdWaitsForAuthoritativeCancelBeforeTerminalizing() {
        var lifecycle = new OrderLifecycleManager();
        var core = new OmsCoreEngine(lifecycle, new SyntheticOrderEngine());
        var o = order(101, 42); o.setTimeInForce(TimeInForce.GTD); o.setExpiresAtMs(NOW - 1);
        lifecycle.restoreOrder(o);
        var submitter = mock(OmsCoreEngine.ClusterSubmitHandler.class); core.setClusterSubmitHandler(submitter);
        core.checkGtdExpiry(NOW);
        assertFalse(o.isTerminal(), "cancel has not been acknowledged yet");
        assertTrue(o.isCancelRequested());
        verify(submitter).submitCancel(42, 7, 1);
    }
}
