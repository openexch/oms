package com.openexchange.oms.common.enums;

/**
 * Full OMS order lifecycle states.
 * Extends the matching engine's basic NEW/FILLED/CANCELLED/REJECTED
 * with pre-trade states (PENDING_RISK, PENDING_HOLD, PENDING_NEW)
 * and synthetic order states (PENDING_TRIGGER).
 */
public enum OmsOrderStatus {
    /** Order received, undergoing risk checks */
    PENDING_RISK,
    /** Risk passed, placing balance hold */
    PENDING_HOLD,
    /** Hold placed, ready to send to cluster */
    PENDING_NEW,
    /** Sent to cluster and accepted (resting on book) */
    NEW,
    /** Synthetic order waiting for trigger condition (stop, trailing) */
    PENDING_TRIGGER,
    /** Partially filled, remainder resting */
    PARTIALLY_FILLED,
    /** Fully filled */
    FILLED,
    /** Cancelled by user or system */
    CANCELLED,
    /** GTD order expired */
    EXPIRED,
    /** Rejected by risk engine, balance check, or cluster */
    REJECTED;

    public boolean isTerminal() {
        return this == FILLED || this == CANCELLED || this == EXPIRED || this == REJECTED;
    }

    public boolean isActive() {
        return !isTerminal();
    }
}
