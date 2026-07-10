// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

import com.openexchange.assets.infrastructure.generated.DepositDecoder;
import com.openexchange.assets.infrastructure.generated.HoldDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionDecoder;
import com.openexchange.assets.infrastructure.generated.ReleaseDecoder;
import com.openexchange.assets.infrastructure.generated.RequestBalanceSnapshotDecoder;
import com.openexchange.assets.infrastructure.generated.RequestHoldSnapshotDecoder;
import com.openexchange.assets.infrastructure.generated.WithdrawDecoder;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Encode-side coverage for {@link AssetsClusterClient}: each {@code submit*} method must write a
 * correctly framed money-schema v2 message. Every test round-trips the queued (pre-encoded) bytes
 * back through the REAL generated decoders and asserts the template id and every field, so a wrong
 * offset, field order, or type would fail here. No live cluster is booted — the client is used purely
 * as an encoder+queue via the {@code copyNextQueuedForTest} seam.
 */
class AssetsClusterClientEncodeTest {

    private final AssetsClusterClient client = new AssetsClusterClient();
    private final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
    private final MessageHeaderDecoder header = new MessageHeaderDecoder();

    /** Drain the single queued command into {@link #buf}; fail if nothing was queued. */
    private void drainOne() {
        int len = client.copyNextQueuedForTest(buf);
        assertTrue(len > 0, "expected exactly one queued command");
        header.wrap(buf, 0);
        assertEquals(2, header.schemaId(), "must be money-schema id 2");
    }

    @Test
    void holdEncodesAndRoundTrips() {
        assertTrue(client.submitHold(111L, 222L, 333L, 7, 5_00000000L, true));
        drainOne();
        assertEquals(HoldDecoder.TEMPLATE_ID, header.templateId());
        HoldDecoder d = new HoldDecoder();
        d.wrapAndApplyHeader(buf, 0, header);
        assertEquals(111L, d.correlationId());
        assertEquals(222L, d.orderId());
        assertEquals(333L, d.userId());
        assertEquals(7, d.assetId());
        assertEquals(5_00000000L, d.amount());
        assertEquals(com.openexchange.assets.infrastructure.generated.BoolFlag.TRUE, d.omsManagedRelease());
    }

    @Test
    void releaseEncodesFullResidualSentinel() {
        // amount = -1 is the "release all remaining" sentinel; it must survive as a signed int64.
        assertTrue(client.submitRelease(222L, 333L, -1L));
        drainOne();
        assertEquals(ReleaseDecoder.TEMPLATE_ID, header.templateId());
        ReleaseDecoder d = new ReleaseDecoder();
        d.wrapAndApplyHeader(buf, 0, header);
        assertEquals(222L, d.orderId());
        assertEquals(333L, d.userId());
        assertEquals(-1L, d.amount());
    }

    @Test
    void releaseEncodesPartialAmount() {
        assertTrue(client.submitRelease(9L, 8L, 250_00000000L));
        drainOne();
        ReleaseDecoder d = new ReleaseDecoder();
        d.wrapAndApplyHeader(buf, 0, header);
        assertEquals(9L, d.orderId());
        assertEquals(8L, d.userId());
        assertEquals(250_00000000L, d.amount());
    }

    @Test
    void depositEncodesAndRoundTrips() {
        assertTrue(client.submitDeposit(42L, 1001L, 3, 1_000_00000000L));
        drainOne();
        assertEquals(DepositDecoder.TEMPLATE_ID, header.templateId());
        DepositDecoder d = new DepositDecoder();
        d.wrapAndApplyHeader(buf, 0, header);
        assertEquals(42L, d.correlationId());
        assertEquals(1001L, d.userId());
        assertEquals(3, d.assetId());
        assertEquals(1_000_00000000L, d.amount());
    }

    @Test
    void withdrawEncodesAndRoundTrips() {
        assertTrue(client.submitWithdraw(43L, 1002L, 5, 12_34500000L));
        drainOne();
        assertEquals(WithdrawDecoder.TEMPLATE_ID, header.templateId());
        WithdrawDecoder d = new WithdrawDecoder();
        d.wrapAndApplyHeader(buf, 0, header);
        assertEquals(43L, d.correlationId());
        assertEquals(1002L, d.userId());
        assertEquals(5, d.assetId());
        assertEquals(12_34500000L, d.amount());
    }

    @Test
    void queryFeedPositionEncodesCorrelation() {
        assertTrue(client.submitQueryFeedPosition(7777L));
        drainOne();
        assertEquals(QueryFeedPositionDecoder.TEMPLATE_ID, header.templateId());
        QueryFeedPositionDecoder d = new QueryFeedPositionDecoder();
        d.wrapAndApplyHeader(buf, 0, header);
        assertEquals(7777L, d.correlationId());
    }

    @Test
    void requestBalanceSnapshotEncodesCorrelation() {
        assertTrue(client.submitRequestBalanceSnapshot(8888L));
        drainOne();
        assertEquals(RequestBalanceSnapshotDecoder.TEMPLATE_ID, header.templateId());
        RequestBalanceSnapshotDecoder d = new RequestBalanceSnapshotDecoder();
        d.wrapAndApplyHeader(buf, 0, header);
        assertEquals(8888L, d.correlationId());
    }

    @Test
    void requestHoldSnapshotEncodesCorrelation() {
        assertTrue(client.submitRequestHoldSnapshot(9999L));
        drainOne();
        assertEquals(RequestHoldSnapshotDecoder.TEMPLATE_ID, header.templateId());
        RequestHoldSnapshotDecoder d = new RequestHoldSnapshotDecoder();
        d.wrapAndApplyHeader(buf, 0, header);
        assertEquals(9999L, d.correlationId());
    }

    @Test
    void queueIsFifoAndDrainsEmpty() {
        assertTrue(client.submitHold(1L, 1L, 1L, 1, 1L, false));
        assertTrue(client.submitDeposit(2L, 2L, 2, 2L));

        // First out is the Hold, second is the Deposit (FIFO), then the queue is empty.
        assertTrue(client.copyNextQueuedForTest(buf) > 0);
        header.wrap(buf, 0);
        assertEquals(HoldDecoder.TEMPLATE_ID, header.templateId());

        assertTrue(client.copyNextQueuedForTest(buf) > 0);
        header.wrap(buf, 0);
        assertEquals(DepositDecoder.TEMPLATE_ID, header.templateId());

        assertEquals(-1, client.copyNextQueuedForTest(buf), "queue must now be empty");
    }
}
