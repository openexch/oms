// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.openexchange.oms.core.OmsCoreEngine;
import com.openexchange.oms.core.OrderLifecycleManager;
import com.openexchange.oms.core.SyntheticOrderEngine;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for the raw engine {@code OrderRejectReason} code to OMS rejectReason string mapping
 * (match#75) and the Layer 2 egressSeq reorder metric.
 *
 * <p>The rejectReason mapping is the OMS-side vocabulary translation and must be total: every code
 * maps to a string or null, and no code (including unknown/future ones) throws.</p>
 */
class OmsEgressAdapterTest {

    // ---- Layer 2: egressSeq reorder metric ----
    // egressSeq is an ORDER KEY ONLY: monotonic non-decreasing in cluster-log order, but sparse
    // (gaps normal) and non-unique (ties normal). It must NEVER be used for dense gap detection.
    // These guardrails pin the metric semantics; they are deliberately kept separate from the
    // statusSeq/tradeId gap-detection tests, whose logic must stay untouched.

    private long nextTradeId = 1;

    /** A fresh adapter over throwaway core state (omsOrderId 0 in the drivers → no side effects). */
    private static OmsEgressAdapter newAdapter() {
        OmsCoreEngine coreEngine = new OmsCoreEngine(new OrderLifecycleManager(), new SyntheticOrderEngine());
        return new OmsEgressAdapter(coreEngine, new OmsMarketDataProvider());
    }

    /** Drive one status egress carrying {@code egressSeq}. omsOrderId 0 → core returns early;
     *  statusSeq 0 → the statusSeq gap block is skipped, isolating the egressSeq metric. */
    private static void status(OmsEgressAdapter a, long egressSeq) {
        a.onOrderStatusUpdate(1, 1L, 1L, 0, 0L, 0L, 0L, true, 0L, 0L, 0, egressSeq);
    }

    /** Drive one trade egress carrying {@code egressSeq}. omsOrderIds 0 → no fill/settle side effects. */
    private void trade(OmsEgressAdapter a, long egressSeq) {
        a.onTradeExecution(1, nextTradeId++, 1L, 2L, 1L, 2L, 1L, 1L, true, 0L, 0L, egressSeq);
    }

    @Test
    void outOfOrderNonZeroEgressSeqIncrementsReorderCount() {
        OmsEgressAdapter a = newAdapter();
        status(a, 100L);              // first non-zero: adopts the high-water mark, not a reorder
        assertEquals(0L, a.getEgressReorderCount());
        assertEquals(100L, a.getLastEgressSeq());

        status(a, 150L);              // monotonic advance: no reorder, high-water mark moves up
        assertEquals(0L, a.getEgressReorderCount());
        assertEquals(150L, a.getLastEgressSeq());

        status(a, 50L);               // below the high-water mark: a reorder
        assertEquals(1L, a.getEgressReorderCount());
        assertEquals(150L, a.getLastEgressSeq(), "a reorder must NOT regress the high-water mark");

        trade(a, 40L);                // reorders count across the trade path too (shared order key)
        assertEquals(2L, a.getEgressReorderCount());
        assertEquals(150L, a.getLastEgressSeq());
    }

    @Test
    void zeroEgressSeqNeverIncrementsAndNeverRegressesLastSeen() {
        OmsEgressAdapter fresh = newAdapter();
        status(fresh, 0L);            // v6/absent on a pristine adapter: no metric, no advance
        assertEquals(0L, fresh.getEgressReorderCount());
        assertEquals(0L, fresh.getLastEgressSeq());

        OmsEgressAdapter a = newAdapter();
        status(a, 100L);
        status(a, 0L);                // absent: skipped entirely — not counted as a reorder below 100
        trade(a, 0L);
        assertEquals(0L, a.getEgressReorderCount());
        assertEquals(100L, a.getLastEgressSeq(), "an absent (0) egressSeq must not disturb the high-water mark");
    }

    @Test
    void tiedEgressSeqValuesDoNotIncrement() {
        OmsEgressAdapter a = newAdapter();
        status(a, 100L);              // all events of one command share one egressSeq: ties are normal
        status(a, 100L);
        trade(a, 100L);
        assertEquals(0L, a.getEgressReorderCount(), "equal egressSeq (a tie) must not count as a reorder");
        assertEquals(100L, a.getLastEgressSeq());
    }

    @Test
    void sentinelAndNoneMapToNull() {
        // -1 sentinel = pre-v6 stream or SBE null (unknown / upstream too old to say).
        assertNull(OmsEgressAdapter.mapRejectReason(-1));
        // 0 = NONE, carried on every non-reject status.
        assertNull(OmsEgressAdapter.mapRejectReason(0));
    }

    @Test
    void everyEngineCodeMapsToItsName() {
        // Mirrors match's OrderRejectReason (verified against the enum on main).
        assertEquals("PRICE_OUT_OF_RANGE", OmsEgressAdapter.mapRejectReason(1));
        assertEquals("PRICE_OFF_TICK", OmsEgressAdapter.mapRejectReason(2));
        assertEquals("LEVEL_FULL", OmsEgressAdapter.mapRejectReason(3));
        assertEquals("BOOK_FULL", OmsEgressAdapter.mapRejectReason(4));
        assertEquals("OVERFLOW", OmsEgressAdapter.mapRejectReason(5));
        assertEquals("INVALID_QUANTITY", OmsEgressAdapter.mapRejectReason(6));
        assertEquals("MATCH_LIMIT", OmsEgressAdapter.mapRejectReason(7));
        assertEquals("WOULD_CROSS", OmsEgressAdapter.mapRejectReason(8));
        assertEquals("NO_LIQUIDITY", OmsEgressAdapter.mapRejectReason(9));
        assertEquals("ORDER_NOT_FOUND", OmsEgressAdapter.mapRejectReason(10));
    }

    @Test
    void unknownCodeFallsBackAndNeverThrows() {
        // A newer engine could introduce codes the OMS does not know yet: surface them verbatim
        // rather than throwing and breaking egress dispatch.
        assertEquals("ENGINE_REJECT_11", OmsEgressAdapter.mapRejectReason(11));
        assertEquals("ENGINE_REJECT_42", OmsEgressAdapter.mapRejectReason(42));
        // 254 is the last non-null code (255 is the SBE null, normalized to -1 upstream).
        assertEquals("ENGINE_REJECT_254", OmsEgressAdapter.mapRejectReason(254));
    }
}
