package com.openexchange.oms.core;

import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;

/**
 * Listener for order state transitions.
 * Used by the API layer to push updates to clients.
 */
public interface OrderStateListener {

    /**
     * Called when an order transitions from one state to another.
     *
     * @param order     the order (with updated state)
     * @param oldStatus the previous status
     * @param newStatus the new status
     */
    void onOrderStateChanged(OmsOrder order, OmsOrderStatus oldStatus, OmsOrderStatus newStatus);
}
