// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.openexchange.oms.common.enums.OmsOrderStatus;

/**
 * Decides which order status transitions release the resources an open order held:
 * its per-user open-order SLOT (risk bookkeeping) and its balance HOLD.
 *
 * <p>oms#111: FILLED used to release neither. The fill path checked pre-fill
 * {@code remainingQty == 0} (never true for a filling order) and the terminal-status
 * listener only released on the cancel family — so every fully-filled order leaked one
 * open-order slot, drifting {@code openOrderCounts} up at ~the fill rate until users hit
 * the cap and were wrongly rejected with OPEN_ORDER_LIMIT. The slot is now released on
 * EVERY terminal, incl. FILLED; the hold is released only when the order terminated
 * without filling (a filled order's hold is consumed by settlement).</p>
 */
final class OpenOrderSlotPolicy {

    private OpenOrderSlotPolicy() {}

    /**
     * True if {@code oldStatus} is a state that holds a slot + hold (every state from
     * PENDING_NEW onward, since the hold is placed before PENDING_NEW).
     */
    static boolean heldResources(OmsOrderStatus oldStatus) {
        return oldStatus == OmsOrderStatus.NEW
                || oldStatus == OmsOrderStatus.PARTIALLY_FILLED
                || oldStatus == OmsOrderStatus.PENDING_NEW
                || oldStatus == OmsOrderStatus.PENDING_TRIGGER;
    }

    /** The open-order slot is freed on EVERY terminal transition out of a holding state. */
    static boolean releasesSlot(OmsOrderStatus oldStatus, OmsOrderStatus newStatus) {
        return heldResources(oldStatus) && newStatus.isTerminal();
    }

    /**
     * The balance hold is released only when the order terminated WITHOUT filling
     * (CANCELLED / EXPIRED / REJECTED). A FILLED order's hold is consumed by settlement;
     * an iceberg FILLED residual is released separately in the terminal cleanup.
     */
    static boolean releasesHold(OmsOrderStatus oldStatus, OmsOrderStatus newStatus) {
        return heldResources(oldStatus)
                && (newStatus == OmsOrderStatus.CANCELLED
                    || newStatus == OmsOrderStatus.EXPIRED
                    || newStatus == OmsOrderStatus.REJECTED);
    }
}
