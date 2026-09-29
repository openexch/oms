// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.core;

import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.common.enums.OmsOrderType;
import com.openexchange.oms.common.enums.OrderSide;
import com.openexchange.oms.common.enums.TimeInForce;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cutover mode: fills and terminals come only from the verified ME journal. Live egress may link
 * the cluster order and move PENDING_NEW to NEW, but never changes fill quantity or ends an order.
 */
class JournalAuthorityTest {
    private static final long QTY = 100_000_000L;
    private OrderLifecycleManager lcm;

    @BeforeEach
    void setUp() {
        lcm = new OrderLifecycleManager();
        lcm.setJournalAuthoritative(true);
    }

    private OmsOrder pendingNew(long id, OmsOrderType type) {
        OmsOrder order = new OmsOrder();
        order.setOmsOrderId(id); order.setUserId(100L); order.setMarketId(1);
        order.setSide(OrderSide.BUY); order.setOrderType(type); order.setTimeInForce(TimeInForce.GTC);
        order.setPrice(100_000_000L); order.setQuantity(QTY); order.setRemainingQty(QTY);
        lcm.registerOrder(order); lcm.onRiskPassed(id); lcm.onHoldPlaced(id);
        return order;
    }

    @Test
    void egressLinksAndAcceptsButNeverFillsOrTerminates() {
        OmsOrder order = pendingNew(1, OmsOrderType.LIMIT);
        lcm.onClusterOrderStatus(1, 500, 0, QTY, 0);
        assertEquals(500, order.getClusterOrderId());
        assertEquals(OmsOrderStatus.NEW, order.getStatus());
        lcm.onClusterOrderStatus(1, 500, 1, QTY / 2, QTY / 2);
        assertEquals(0, order.getFilledQty(), "egress status must not raise fills");
        assertEquals(OmsOrderStatus.NEW, order.getStatus());
        for (int terminal = 2; terminal <= 4; terminal++) {
            lcm.onClusterOrderStatus(1, 500, terminal, 0, QTY, null);
            assertSame(order, lcm.getOrder(1), "egress status " + terminal + " must not end the order");
            assertEquals(OmsOrderStatus.NEW, order.getStatus());
        }
    }

    @Test
    void journalTerminalEndsTheOrderAndKeepsJournalFills() {
        OmsOrder order = pendingNew(2, OmsOrderType.LIMIT);
        lcm.applyFill(2, 501, QTY / 4);
        assertSame(order, lcm.onJournalTerminal(2, 501, 3));
        assertEquals(OmsOrderStatus.CANCELLED, order.getStatus());
        assertEquals(QTY / 4, order.getFilledQty());
        assertNull(lcm.getOrder(2));
        assertNull(lcm.onJournalTerminal(2, 501, 3), "a repeated terminal is a no-op");
    }

    @Test
    void journalRejectTerminatesAnUnacknowledgedOrder() {
        OmsOrder order = pendingNew(3, OmsOrderType.LIMIT);
        lcm.onJournalTerminal(3, 502, 4);
        assertEquals(OmsOrderStatus.REJECTED, order.getStatus());
        assertEquals(502, order.getClusterOrderId());
    }

    @Test
    void journalSliceFilledDoesNotEndTheIcebergParent() {
        OmsOrder order = pendingNew(4, OmsOrderType.ICEBERG);
        order.setDisplayQuantity(QTY / 4); order.setHiddenQuantity(QTY);
        lcm.applyFill(4, 503, QTY / 4);
        lcm.onJournalTerminal(4, 503, 2);
        assertSame(order, lcm.getOrder(4));
        assertEquals(0, order.getClusterOrderId(), "the finished slice is unlinked");
        assertEquals(QTY / 4, order.getFilledQty());
    }

    @Test
    void journalTerminalRejectsNonTerminalStatus() {
        pendingNew(5, OmsOrderType.LIMIT);
        assertThrows(IllegalArgumentException.class, () -> lcm.onJournalTerminal(5, 504, 1));
    }

    @Test
    void legacyModeStillTakesTerminalsFromEgress() {
        lcm.setJournalAuthoritative(false);
        OmsOrder order = pendingNew(6, OmsOrderType.LIMIT);
        lcm.onClusterOrderStatus(6, 505, 3, QTY, 0);
        assertEquals(OmsOrderStatus.CANCELLED, order.getStatus());
    }
}
