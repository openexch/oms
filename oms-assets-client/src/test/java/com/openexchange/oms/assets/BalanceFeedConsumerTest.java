// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

import com.openexchange.assets.infrastructure.generated.BalanceUpdateBatchEncoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateEncoder;
import com.openexchange.assets.infrastructure.generated.DepositAckEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.SubscribeDecoder;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit coverage for the balance-feed consumer seams of {@link AssetsClusterClient}, no cluster:
 * the exact bytes of the {@code Subscribe} narrowing, the re-declare trigger on a leader change,
 * and the feed-fragment dispatch path (same listener callbacks, cluster egress counters untouched).
 */
class BalanceFeedConsumerTest {

    private static final String INGRESS = "0=localhost:9302";
    private static final String EGRESS = "aeron:udp?endpoint=localhost:9393";
    private static final String FEED = "aeron:udp?endpoint=localhost:9494";

    private static AssetsClusterClient feedClient() {
        return new AssetsClusterClient(INGRESS, EGRESS, FEED,
                AssetsClusterClient.DEFAULT_BALANCE_FEED_STREAM_ID);
    }

    /** Records only what these tests assert; the rest of the listener surface is no-op. */
    private static final class Recording implements AssetsEgressListener {
        final List<long[]> balanceUpdates = new ArrayList<>();
        int otherCallbacks;

        @Override
        public void onBalanceUpdate(long userId, int assetId, long available, long locked) {
            balanceUpdates.add(new long[] {userId, assetId, available, locked});
        }

        @Override
        public void onHoldAck(long c, long o, long u, int a, long am) { otherCallbacks++; }
        @Override
        public void onHoldReject(long c, long o, long u, int a, long am, int r) { otherCallbacks++; }
        @Override
        public void onDepositAck(long c, long u, int a, long am, long n) { otherCallbacks++; }
        @Override
        public void onWithdrawAck(long c, long u, int a, long am, long n) { otherCallbacks++; }
        @Override
        public void onWithdrawReject(long c, long u, int a, long am, int r) { otherCallbacks++; }
        @Override
        public void onSettlementApplied(long t, long b, long s) { otherCallbacks++; }
        @Override
        public void onFeedPositionReport(long c, long p, long t) { otherCallbacks++; }
        @Override
        public void onBalanceSnapshotEnd(long c, int e) { otherCallbacks++; }
        @Override
        public void onHoldSnapshotEntry(long o, long u, int a, long r) { otherCallbacks++; }
        @Override
        public void onHoldSnapshotEnd(long c, int e) { otherCallbacks++; }
        @Override
        public void onConnected() { }
        @Override
        public void onDisconnected() { }
    }

    // ==================== Subscribe frame ====================

    @Test
    void subscribeFrameNarrowsToAcksAndSettlementsOnly() {
        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final int length = feedClient().copySubscribeForTest(buf);
        assertTrue(length > 0, "a feed-configured client must carry a Subscribe frame");

        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        header.wrap(buf, 0);
        assertEquals(SubscribeDecoder.TEMPLATE_ID, header.templateId());

        final SubscribeDecoder subscribe = new SubscribeDecoder();
        subscribe.wrapAndApplyHeader(buf, 0, header);
        assertEquals(0L, subscribe.correlationId());
        assertTrue(subscribe.channels().acks(), "acks must stay on the session (callers block on them)");
        assertTrue(subscribe.channels().settlements(), "settlements must stay on the session");
        assertFalse(subscribe.channels().balances(), "balances move to the feed side-channel");
        assertFalse(subscribe.channels().snapshots(),
                "snapshot replies are origin-routed and mask-exempt (assets#56); no broadcast subscription needed");
        assertEquals(MessageHeaderEncoder.ENCODED_LENGTH + subscribe.encodedLength(), length);
    }

    @Test
    void noSubscribeFrameWhenFeedOff() {
        final AssetsClusterClient legacy = new AssetsClusterClient(INGRESS, EGRESS);
        assertEquals(-1, legacy.copySubscribeForTest(new ExpandableArrayBuffer()),
                "feed off = legacy behavior: no Subscribe exists to send");
    }

    // ==================== Re-declare trigger ====================

    @Test
    void newLeaderArmsSubscribeRedeclareOnlyWhenFeedConfigured() {
        final AssetsClusterClient withFeed = feedClient();
        assertFalse(withFeed.isSubscribePending(), "nothing armed before a session exists");
        withFeed.onNewLeader(1L, 2L, 1, "0=localhost:9302");
        assertTrue(withFeed.isSubscribePending(),
                "leader-local transport state must be re-declared on every leader change");

        final AssetsClusterClient legacy = new AssetsClusterClient(INGRESS, EGRESS);
        legacy.onNewLeader(1L, 2L, 1, "0=localhost:9302");
        assertFalse(legacy.isSubscribePending(), "feed off: byte-identical legacy behavior, no Subscribe");
    }

    // ==================== Feed fragment dispatch ====================

    @Test
    void feedFragmentRoutesSingleBalanceUpdateThroughTheListenerPath() {
        final AssetsClusterClient client = feedClient();
        final Recording rec = new Recording();
        client.setEgressListener(rec);

        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final BalanceUpdateEncoder enc = new BalanceUpdateEncoder();
        enc.wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder())
                .userId(42L).assetId(3).available(1_000L).locked(250L);
        client.onFeedFragment(buf, 0, MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength(), null);

        assertEquals(1, rec.balanceUpdates.size());
        assertEquals(0, rec.otherCallbacks);
        final long[] u = rec.balanceUpdates.get(0);
        assertEquals(42L, u[0]);
        assertEquals(3L, u[1]);
        assertEquals(1_000L, u[2]);
        assertEquals(250L, u[3]);
        assertEquals(1, client.getFeedFramesReceived());
        assertEquals(1, client.getFeedEntriesReceived());
        // The side-channel must not masquerade as cluster egress: the stale-egress watchdog and the
        // firehose counter reason about the request-response session only.
        assertEquals(0, client.getEgressMessageCount());
        assertEquals(0, client.getEgressBalanceUpdateEntries());
    }

    @Test
    void feedFragmentRoutesBatchEntriesIndividually() {
        final AssetsClusterClient client = feedClient();
        final Recording rec = new Recording();
        client.setEgressListener(rec);

        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final BalanceUpdateBatchEncoder enc = new BalanceUpdateBatchEncoder();
        final BalanceUpdateBatchEncoder.UpdatesEncoder updates =
                enc.wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder()).updatesCount(2);
        updates.next().userId(1L).assetId(0).available(10L).locked(1L);
        updates.next().userId(2L).assetId(1).available(20L).locked(2L);
        client.onFeedFragment(buf, 0, MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength(), null);

        assertEquals(2, rec.balanceUpdates.size());
        assertEquals(1, client.getFeedFramesReceived());
        assertEquals(2, client.getFeedEntriesReceived());
        assertEquals(2L, rec.balanceUpdates.get(1)[0]);
        assertEquals(20L, rec.balanceUpdates.get(1)[2]);
        assertEquals(0, client.getEgressBalanceUpdateEntries());
    }

    @Test
    void feedFragmentIgnoresForeignTemplates() {
        final AssetsClusterClient client = feedClient();
        final Recording rec = new Recording();
        client.setEgressListener(rec);

        final ExpandableArrayBuffer buf = new ExpandableArrayBuffer();
        final DepositAckEncoder enc = new DepositAckEncoder();
        enc.wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder())
                .correlationId(9L).userId(1L).assetId(0).amount(5L).newAvailable(5L);
        client.onFeedFragment(buf, 0, MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength(), null);

        assertEquals(0, rec.balanceUpdates.size());
        assertEquals(0, rec.otherCallbacks, "the feed's vocabulary is balance frames only");
        assertEquals(0, client.getFeedFramesReceived());
    }
}
