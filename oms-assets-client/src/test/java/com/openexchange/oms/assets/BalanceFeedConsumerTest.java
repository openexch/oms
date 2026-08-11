// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

import com.openexchange.assets.infrastructure.generated.BalanceUpdateBatchEncoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateEncoder;
import com.openexchange.assets.infrastructure.generated.DepositAckEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.SubscribeDecoder;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit coverage for the balance-feed consumer seams of {@link AssetsClusterClient}, no cluster:
 * the exact bytes of the {@code Subscribe} narrowing, the re-declare trigger on a leader change,
 * the feed-fragment dispatch path (same listener callbacks, cluster egress counters untouched),
 * and the comma-separated multi-URI subscription set (one subscription per AE node endpoint —
 * see {@code AssetsClusterClient#balanceFeedChannels} for why a list) against a real driver.
 */
class BalanceFeedConsumerTest {

    private static final String INGRESS = "0=localhost:9302";
    private static final String EGRESS = "aeron:udp?endpoint=localhost:9393";
    private static final String FEED = "aeron:udp?endpoint=localhost:9494";

    /** Two feed endpoints standing in for two AE nodes' control endpoints (ports unused elsewhere). */
    private static final String FEED_A = "aeron:udp?endpoint=localhost:19710";
    private static final String FEED_B = "aeron:udp?endpoint=localhost:19711";

    private static final long AWAIT_TIMEOUT_MS = 10_000;

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

    // ==================== Comma-separated URI list ====================

    @Test
    void allEmptyChannelListMeansFeedOff() {
        // Parser contract: split on ',', trim, drop empties — a value of only separators and
        // whitespace configures NO feed, byte-identical to the legacy empty-channel client.
        final AssetsClusterClient client = new AssetsClusterClient(INGRESS, EGRESS, " , ,",
                AssetsClusterClient.DEFAULT_BALANCE_FEED_STREAM_ID);
        assertEquals(-1, client.copySubscribeForTest(new ExpandableArrayBuffer()),
                "an all-empty channel list must mean feed off (no Subscribe frame)");
        client.onNewLeader(1L, 2L, 1, INGRESS);
        assertFalse(client.isSubscribePending(), "feed off: a leader change must arm nothing");
    }

    /**
     * The multi-URI path against a REAL driver: a messy comma list opens one subscription per URI
     * (whitespace trimmed, empties dropped), frames published on EITHER endpoint reach the same
     * listener path, one {@code pollFeedSubscriptions()} cycle never exceeds the shared TOTAL
     * fragment budget across all subscriptions, and teardown closes every subscription.
     */
    @Test
    void commaListOpensOneSubscriptionPerUriAndEitherDelivers() {
        final String aeronDir = new File(System.getProperty("java.io.tmpdir"),
                "oms-feed-multi-" + ProcessHandle.current().pid()).getAbsolutePath();

        try (MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
                     .aeronDirectoryName(aeronDir)
                     .threadingMode(ThreadingMode.SHARED)
                     .publicationTermBufferLength(64 * 1024)
                     .ipcTermBufferLength(64 * 1024)
                     .dirDeleteOnStart(true)
                     .dirDeleteOnShutdown(true));
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
             AssetsClusterClient client = new AssetsClusterClient(INGRESS, EGRESS,
                     " " + FEED_A + " ,, " + FEED_B + " ,", // messy on purpose: trim + drop empties
                     AssetsClusterClient.DEFAULT_BALANCE_FEED_STREAM_ID)) {

            final Recording rec = new Recording();
            client.setEgressListener(rec);

            client.openFeedSubscriptions(aeron);
            final Subscription[] subs = client.feedSubscriptionsForTest();
            assertEquals(2, subs.length, "one subscription per URI, empties dropped");

            try (Publication pubA = aeron.addPublication(FEED_A,
                         AssetsClusterClient.DEFAULT_BALANCE_FEED_STREAM_ID);
                 Publication pubB = aeron.addPublication(FEED_B,
                         AssetsClusterClient.DEFAULT_BALANCE_FEED_STREAM_ID)) {
                awaitTrue("pubA connected", pubA::isConnected);
                awaitTrue("pubB connected", pubB::isConnected);

                // Frames on EITHER endpoint arrive through the same dispatch path.
                offerBalanceUpdate(pubA, 1L, 0, 100L, 10L);
                awaitTrue("frame from endpoint A", () -> pollOnce(client) >= 0 && rec.balanceUpdates.size() == 1);
                assertEquals(1L, rec.balanceUpdates.get(0)[0]);
                assertEquals(100L, rec.balanceUpdates.get(0)[2]);

                offerBalanceUpdate(pubB, 2L, 1, 200L, 20L);
                awaitTrue("frame from endpoint B", () -> pollOnce(client) >= 0 && rec.balanceUpdates.size() == 2);
                assertEquals(2L, rec.balanceUpdates.get(1)[0]);
                assertEquals(200L, rec.balanceUpdates.get(1)[2]);

                // Budget: FEED_POLL_FRAGMENT_LIMIT (64) is the per-cycle TOTAL across all
                // subscriptions, not per subscription. 40 frames on each endpoint = 80 available;
                // no single poll cycle may consume more than 64, and all 80 must drain.
                for (int i = 0; i < 40; i++) {
                    offerBalanceUpdate(pubA, 100L + i, 0, i, 0L);
                    offerBalanceUpdate(pubB, 200L + i, 1, i, 0L);
                }
                int drained = 0;
                final long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
                while (drained < 80) {
                    assertTrue(System.currentTimeMillis() < deadline, "timed out draining the burst");
                    final int consumed = client.pollFeedSubscriptions();
                    assertTrue(consumed <= 64,
                            "one poll cycle consumed " + consumed + " fragments; 64 is the shared total budget");
                    drained += consumed;
                }
                assertEquals(80, drained);
                assertEquals(82, rec.balanceUpdates.size());
            }

            // Teardown mirrors the single-close: close() must close EVERY subscription.
            client.close();
            for (Subscription sub : subs) {
                assertTrue(sub.isClosed(), "close() must close every feed subscription");
            }
            assertNull(client.feedSubscriptionsForTest(), "the subscription set must be torn down");
        }
    }

    /** One poll on the client's own seam; returns fragments consumed (side effect: dispatch). */
    private static int pollOnce(AssetsClusterClient client) {
        return client.pollFeedSubscriptions();
    }

    private static void offerBalanceUpdate(Publication pub, long userId, int assetId,
                                           long available, long locked) {
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[64]);
        final BalanceUpdateEncoder enc = new BalanceUpdateEncoder();
        enc.wrapAndApplyHeader(buf, 0, new MessageHeaderEncoder())
                .userId(userId).assetId(assetId).available(available).locked(locked);
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + enc.encodedLength();
        final long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (pub.offer(buf, 0, length) < 0) {
            assertTrue(System.currentTimeMillis() < deadline, "offer back-pressured too long");
            Thread.onSpinWait();
        }
    }

    private static void awaitTrue(String what, BooleanSupplier cond) {
        final long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT_MS;
        while (!cond.getAsBoolean()) {
            assertTrue(System.currentTimeMillis() < deadline, "timed out waiting for: " + what);
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
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
