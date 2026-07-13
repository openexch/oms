// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.openexchange.oms.common.enums.OmsOrderStatus;
import org.junit.jupiter.api.Test;

import static com.openexchange.oms.common.enums.OmsOrderStatus.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** oms#111: the open-order slot must be released on EVERY terminal, including FILLED. */
class OpenOrderSlotPolicyTest {

    private static final OmsOrderStatus[] HELD = {NEW, PARTIALLY_FILLED, PENDING_NEW, PENDING_TRIGGER};

    @Test
    void filledReleasesSlotButNotHold_fromEveryHoldingState() {
        // The regression: FILLED released neither, leaking a slot per fill.
        for (OmsOrderStatus held : HELD) {
            assertTrue(OpenOrderSlotPolicy.releasesSlot(held, FILLED),
                    "FILLED from " + held + " must release the open-order slot");
            assertFalse(OpenOrderSlotPolicy.releasesHold(held, FILLED),
                    "FILLED from " + held + " must NOT releaseForCancel (settlement consumed the hold)");
        }
    }

    @Test
    void cancelFamilyReleasesBothSlotAndHold() {
        for (OmsOrderStatus held : HELD) {
            for (OmsOrderStatus term : new OmsOrderStatus[]{CANCELLED, EXPIRED, REJECTED}) {
                assertTrue(OpenOrderSlotPolicy.releasesSlot(held, term), term + " from " + held + " releases slot");
                assertTrue(OpenOrderSlotPolicy.releasesHold(held, term), term + " from " + held + " releases hold");
            }
        }
    }

    @Test
    void nonTerminalTransitionsReleaseNothing() {
        assertFalse(OpenOrderSlotPolicy.releasesSlot(PENDING_NEW, NEW));
        assertFalse(OpenOrderSlotPolicy.releasesSlot(NEW, PARTIALLY_FILLED));
        assertFalse(OpenOrderSlotPolicy.releasesHold(NEW, PARTIALLY_FILLED));
    }

    @Test
    void terminalFromNonHoldingStateReleasesNothing() {
        // A pre-hold state (PENDING_RISK/PENDING_HOLD) never acquired a slot or hold.
        assertFalse(OpenOrderSlotPolicy.releasesSlot(PENDING_RISK, REJECTED));
        assertFalse(OpenOrderSlotPolicy.releasesHold(PENDING_HOLD, REJECTED));
        assertFalse(OpenOrderSlotPolicy.releasesSlot(PENDING_RISK, FILLED));
    }
}
