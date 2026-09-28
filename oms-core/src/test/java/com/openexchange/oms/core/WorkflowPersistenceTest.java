// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.core;

import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.*;
import org.junit.jupiter.api.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class WorkflowPersistenceTest {
    private final OrderLifecycleManager lifecycle = new OrderLifecycleManager();
    private final SyntheticOrderEngine synthetic = new SyntheticOrderEngine();
    private final OmsCoreEngine core = new OmsCoreEngine(lifecycle, synthetic);
    private final OmsCoreEngine.PersistenceHandler persistence = mock(OmsCoreEngine.PersistenceHandler.class);
    private final OmsCoreEngine.ClusterSubmitHandler submit = mock(OmsCoreEngine.ClusterSubmitHandler.class);
    @BeforeEach void wire() {
        core.setPersistenceHandler(persistence); core.setClusterSubmitHandler(submit);
        when(submit.submitIcebergSlice(any(), anyLong())).thenReturn(true);
        when(submit.submitTriggeredOrder(any(), any(), anyLong())).thenReturn(true);
    }
    private OmsOrder armed(OmsOrderType type) {
        var o = new OmsOrder(); o.setOmsOrderId(77); o.setUserId(7); o.setMarketId(1);
        o.setOrderType(type); o.setSide(OrderSide.SELL); o.setTimeInForce(TimeInForce.GTC);
        o.setQuantity(100); o.setRemainingQty(100); o.setPrice(990); o.setStopPrice(950);
        o.setTrailingArmPrice(1000); o.setTrailingDelta(50);
        lifecycle.registerOrder(o); lifecycle.onRiskPassed(77); lifecycle.onHoldPlaced(77);
        if (type != OmsOrderType.ICEBERG) lifecycle.onPendingTrigger(77);
        synthetic.registerOrder(o); return o;
    }
    @Test void stopSubmitRequiresCommittedTransitionOutOfArmedState() {
        var o = armed(OmsOrderType.STOP_LIMIT);
        var states = new ArrayList<OmsOrderStatus>();
        doAnswer(c -> { states.add(o.getStatus()); return null; }).when(persistence).persistOrderUpdate(o);
        core.onMarketDataUpdate(1, 940, 941);
        assertEquals(List.of(OmsOrderStatus.PENDING_NEW), states);
        var ordered = inOrder(persistence, submit);
        ordered.verify(persistence).persistOrderUpdate(o);
        ordered.verify(submit).submitTriggeredOrder(o, OmsOrderType.LIMIT, 990);
    }
    @Test void failedTriggerCommitCannotReachMatchingEngine() {
        var o = armed(OmsOrderType.STOP_LIMIT);
        doThrow(new IllegalStateException("database unavailable")).when(persistence).persistOrderUpdate(o);
        assertThrows(IllegalStateException.class, () -> core.onMarketDataUpdate(1, 940, 941));
        verifyNoInteractions(submit);
    }
    @Test void icebergSliceIntentPrecedesSubmission() {
        var o = armed(OmsOrderType.ICEBERG); o.setDisplayQuantity(10); o.setHiddenQuantity(100);
        doAnswer(c -> { assertEquals(10, o.getSliceRemainingQty()); return null; })
                .when(persistence).persistOrderUpdate(o);
        assertTrue(core.submitIcebergSlice(o, 10));
        var ordered = inOrder(persistence, submit);
        ordered.verify(persistence).persistOrderUpdate(o);
        ordered.verify(submit).submitIcebergSlice(o, 10);
    }
    @Test void trailingExtremeIsCommittedBeforeSubsequentTrigger() {
        var o = armed(OmsOrderType.TRAILING_STOP);
        var extremes = new ArrayList<Long>();
        doAnswer(c -> { extremes.add(o.getTrailingArmPrice()); return null; }).when(persistence).persistOrderUpdate(o);
        core.onMarketDataUpdate(1, 1100, 1101);
        assertEquals(List.of(1100L), extremes);
        verifyNoInteractions(submit);
    }
    @Test void missingBidDoesNotFireSellTrailingOrder() {
        var o = armed(OmsOrderType.TRAILING_STOP);
        core.onMarketDataUpdate(1, 0, 1101);
        verifyNoInteractions(submit);
        assertEquals(1000, o.getTrailingArmPrice());
    }
    @Test void writeFailureLatchesRecoveryEvenAfterDatabaseComesBack() {
        var o = armed(OmsOrderType.STOP_LIMIT);
        doThrow(new IllegalStateException("database unavailable")).when(persistence).persistOrderUpdate(o);
        assertThrows(IllegalStateException.class, () -> core.onMarketDataUpdate(1, 940, 941));
        assertFalse(core.isDurableStateHealthy());
        reset(persistence);
        assertThrows(IllegalStateException.class, () -> core.onMarketDataUpdate(1, 930, 931));
        verifyNoInteractions(submit);
    }
    @Test void triggerQueueFailureRemainsVisibleAndCannotRearm() {
        var o = armed(OmsOrderType.STOP_LIMIT);
        when(submit.submitTriggeredOrder(any(), any(), anyLong())).thenReturn(false);
        core.onMarketDataUpdate(1, 940, 941);
        assertEquals(OmsOrderStatus.PENDING_NEW, o.getStatus());
        assertEquals(1, core.getUnresolvedOrderCount());
        core.onMarketDataUpdate(1, 930, 931);
        verify(submit, times(1)).submitTriggeredOrder(any(), any(), anyLong());
    }
    @Test void fullRefillQueueDoesNotSilentlyLoseAcceptedParent() {
        var o = armed(OmsOrderType.ICEBERG); o.setDisplayQuantity(10); o.setHiddenQuantity(100);
        when(submit.submitIcebergSlice(any(), anyLong())).thenReturn(false);
        synthetic.onIcebergSliceFilled(o.getOmsOrderId());
        assertEquals(1, core.getUnresolvedOrderCount());
        assertFalse(o.isTerminal());
        assertEquals(90, o.getHiddenQuantity());
    }
    @Test void gtdCancelIntentIsDurableBeforeSend() {
        var o = armed(OmsOrderType.ICEBERG); o.setTimeInForce(TimeInForce.GTD); o.setExpiresAtMs(1);
        lifecycle.onSentToCluster(o.getOmsOrderId(), 44);
        core.checkGtdExpiry(2);
        var ordered = inOrder(persistence, submit);
        ordered.verify(persistence).persistOrderUpdate(o);
        ordered.verify(submit).submitCancel(44, 7, 1);
        assertTrue(o.isCancelRequested());
    }

}
