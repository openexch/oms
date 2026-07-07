// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for the raw engine {@code OrderRejectReason} code to OMS rejectReason string mapping
 * (match#75). The mapping is the OMS-side vocabulary translation and must be total: every code maps
 * to a string or null, and no code (including unknown/future ones) throws.
 */
class OmsEgressAdapterTest {

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
