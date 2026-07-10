// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

import com.openexchange.assets.infrastructure.generated.BalanceSnapshotEndEncoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateEncoder;
import com.openexchange.assets.infrastructure.generated.DepositAckEncoder;
import com.openexchange.assets.infrastructure.generated.FeedPositionReportEncoder;
import com.openexchange.assets.infrastructure.generated.HoldAckEncoder;
import com.openexchange.assets.infrastructure.generated.HoldRejectEncoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEndEncoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEntryEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.RejectReason;
import com.openexchange.assets.infrastructure.generated.SettlementAppliedEncoder;
import com.openexchange.assets.infrastructure.generated.WithdrawAckEncoder;
import com.openexchange.assets.infrastructure.generated.WithdrawRejectEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Dispatch-side coverage for {@link AssetsClusterClient}: hand-encode each money-schema v2 egress
 * message, feed it through the {@code dispatchEgress} seam (the real body of {@code onMessage}), and
 * assert the {@link AssetsEgressListener} receives the exact template routing and field values. A
 * mis-routed template id or a swapped field would surface as a failed assertion.
 */
class AssetsEgressDispatchTest {

    private final AssetsClusterClient client = new AssetsClusterClient();
    private final Recording rec = new Recording();
    private final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();

    AssetsEgressDispatchTest() {
        client.setEgressListener(rec);
    }

    private void dispatch(int encodedBodyLength) {
        client.dispatchEgress(buf, 0, MessageHeaderEncoder.ENCODED_LENGTH + encodedBodyLength);
    }

    @Test
    void holdAckRoutedWithFields() {
        HoldAckEncoder enc = new HoldAckEncoder();
        enc.wrapAndApplyHeader(buf, 0, header).correlationId(11).orderId(22).userId(33).assetId(4).amount(555);
        dispatch(enc.encodedLength());
        assertEquals("holdAck", rec.last);
        assertArrayEquals(new long[]{11, 22, 33, 4, 555}, rec.values);
    }

    @Test
    void holdRejectRoutedWithRawReason() {
        HoldRejectEncoder enc = new HoldRejectEncoder();
        enc.wrapAndApplyHeader(buf, 0, header)
                .correlationId(1).orderId(2).userId(3).assetId(4).amount(5).reason(RejectReason.INSUFFICIENT_FUNDS);
        dispatch(enc.encodedLength());
        assertEquals("holdReject", rec.last);
        // reason arrives as the raw code (1 = INSUFFICIENT_FUNDS), not the enum ordinal.
        assertArrayEquals(new long[]{1, 2, 3, 4, 5, 1}, rec.values);
    }

    @Test
    void balanceUpdateRouted() {
        BalanceUpdateEncoder enc = new BalanceUpdateEncoder();
        enc.wrapAndApplyHeader(buf, 0, header).userId(77).assetId(9).available(1000).locked(250);
        dispatch(enc.encodedLength());
        assertEquals("balanceUpdate", rec.last);
        assertArrayEquals(new long[]{77, 9, 1000, 250}, rec.values);
    }

    @Test
    void settlementAppliedRouted() {
        SettlementAppliedEncoder enc = new SettlementAppliedEncoder();
        enc.wrapAndApplyHeader(buf, 0, header).tradeId(9001).buyerUserId(5).sellerUserId(6);
        dispatch(enc.encodedLength());
        assertEquals("settlementApplied", rec.last);
        assertArrayEquals(new long[]{9001, 5, 6}, rec.values);
    }

    @Test
    void withdrawRejectRoutedWithRawReason() {
        WithdrawRejectEncoder enc = new WithdrawRejectEncoder();
        enc.wrapAndApplyHeader(buf, 0, header)
                .correlationId(1).userId(2).assetId(3).amount(4).reason(RejectReason.INVALID_AMOUNT);
        dispatch(enc.encodedLength());
        assertEquals("withdrawReject", rec.last);
        assertArrayEquals(new long[]{1, 2, 3, 4, 2}, rec.values); // 2 = INVALID_AMOUNT
    }

    @Test
    void depositAckRouted() {
        DepositAckEncoder enc = new DepositAckEncoder();
        enc.wrapAndApplyHeader(buf, 0, header).correlationId(42).userId(1001).assetId(3).amount(500).newAvailable(1500);
        dispatch(enc.encodedLength());
        assertEquals("depositAck", rec.last);
        assertArrayEquals(new long[]{42, 1001, 3, 500, 1500}, rec.values);
    }

    @Test
    void withdrawAckRouted() {
        WithdrawAckEncoder enc = new WithdrawAckEncoder();
        enc.wrapAndApplyHeader(buf, 0, header).correlationId(43).userId(1002).assetId(5).amount(200).newAvailable(800);
        dispatch(enc.encodedLength());
        assertEquals("withdrawAck", rec.last);
        assertArrayEquals(new long[]{43, 1002, 5, 200, 800}, rec.values);
    }

    @Test
    void feedPositionReportRouted() {
        FeedPositionReportEncoder enc = new FeedPositionReportEncoder();
        enc.wrapAndApplyHeader(buf, 0, header).correlationId(7).consumePosition(123456).lastAppliedTradeId(789);
        dispatch(enc.encodedLength());
        assertEquals("feedPositionReport", rec.last);
        assertArrayEquals(new long[]{7, 123456, 789}, rec.values);
    }

    @Test
    void balanceSnapshotEndRouted() {
        BalanceSnapshotEndEncoder enc = new BalanceSnapshotEndEncoder();
        enc.wrapAndApplyHeader(buf, 0, header).correlationId(55).entryCount(1234);
        dispatch(enc.encodedLength());
        assertEquals("balanceSnapshotEnd", rec.last);
        assertArrayEquals(new long[]{55, 1234}, rec.values);
    }

    @Test
    void holdSnapshotEntryRouted() {
        HoldSnapshotEntryEncoder enc = new HoldSnapshotEntryEncoder();
        enc.wrapAndApplyHeader(buf, 0, header).orderId(10).userId(20).assetId(2).remaining(333);
        dispatch(enc.encodedLength());
        assertEquals("holdSnapshotEntry", rec.last);
        assertArrayEquals(new long[]{10, 20, 2, 333}, rec.values);
    }

    @Test
    void holdSnapshotEndRouted() {
        HoldSnapshotEndEncoder enc = new HoldSnapshotEndEncoder();
        enc.wrapAndApplyHeader(buf, 0, header).correlationId(66).entryCount(7);
        dispatch(enc.encodedLength());
        assertEquals("holdSnapshotEnd", rec.last);
        assertArrayEquals(new long[]{66, 7}, rec.values);
    }

    @Test
    void unknownTemplateIsIgnored() {
        // Frame a header with a templateId the client does not handle (schema ingress id 1 = Deposit,
        // which is not an egress case). dispatchEgress must swallow it without touching the listener.
        header.wrap(buf, 0).blockLength(0).templateId(1).schemaId(2).version(2);
        client.dispatchEgress(buf, 0, MessageHeaderEncoder.ENCODED_LENGTH);
        assertNull(rec.last, "unknown egress template must not invoke any callback");
    }

    @Test
    void shortBufferIsIgnored() {
        client.dispatchEgress(buf, 0, 3); // shorter than a message header
        assertNull(rec.last);
    }

    /** Records the last callback name and its arguments (ints widened to long) for exact assertions. */
    private static final class Recording implements AssetsEgressListener {
        String last;
        long[] values;

        @Override
        public void onHoldAck(long correlationId, long orderId, long userId, int assetId, long amount) {
            last = "holdAck";
            values = new long[]{correlationId, orderId, userId, assetId, amount};
        }

        @Override
        public void onHoldReject(long correlationId, long orderId, long userId, int assetId, long amount, int reasonCode) {
            last = "holdReject";
            values = new long[]{correlationId, orderId, userId, assetId, amount, reasonCode};
        }

        @Override
        public void onBalanceUpdate(long userId, int assetId, long available, long locked) {
            last = "balanceUpdate";
            values = new long[]{userId, assetId, available, locked};
        }

        @Override
        public void onDepositAck(long correlationId, long userId, int assetId, long amount, long newAvailable) {
            last = "depositAck";
            values = new long[]{correlationId, userId, assetId, amount, newAvailable};
        }

        @Override
        public void onWithdrawAck(long correlationId, long userId, int assetId, long amount, long newAvailable) {
            last = "withdrawAck";
            values = new long[]{correlationId, userId, assetId, amount, newAvailable};
        }

        @Override
        public void onWithdrawReject(long correlationId, long userId, int assetId, long amount, int reasonCode) {
            last = "withdrawReject";
            values = new long[]{correlationId, userId, assetId, amount, reasonCode};
        }

        @Override
        public void onSettlementApplied(long tradeId, long buyerUserId, long sellerUserId) {
            last = "settlementApplied";
            values = new long[]{tradeId, buyerUserId, sellerUserId};
        }

        @Override
        public void onFeedPositionReport(long correlationId, long consumePosition, long lastAppliedTradeId) {
            last = "feedPositionReport";
            values = new long[]{correlationId, consumePosition, lastAppliedTradeId};
        }

        @Override
        public void onBalanceSnapshotEnd(long correlationId, int entryCount) {
            last = "balanceSnapshotEnd";
            values = new long[]{correlationId, entryCount};
        }

        @Override
        public void onHoldSnapshotEntry(long orderId, long userId, int assetId, long remaining) {
            last = "holdSnapshotEntry";
            values = new long[]{orderId, userId, assetId, remaining};
        }

        @Override
        public void onHoldSnapshotEnd(long correlationId, int entryCount) {
            last = "holdSnapshotEnd";
            values = new long[]{correlationId, entryCount};
        }

        @Override
        public void onConnected() {
        }

        @Override
        public void onDisconnected() {
        }
    }
}
