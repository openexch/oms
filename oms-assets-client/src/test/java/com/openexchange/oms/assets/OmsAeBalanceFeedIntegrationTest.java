// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

import com.match.domain.FixedPoint;
import com.openexchange.assets.infrastructure.feed.BalanceFeedRuntime;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.DepositAckDecoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.SettleEncoder;
import com.openexchange.assets.infrastructure.persistence.AssetsClusteredService;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import com.openexchange.oms.common.enums.Asset;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PR-3b end-to-end: the REAL {@link AssetsClusterClient} + {@link AeronAssetsBalanceStore} against a
 * REAL in-process single-node AE with the balance-feed side-channel ENABLED (assets#55) and the
 * session narrowed by {@code Subscribe{acks, settlements}} (assets#54/#56). Sibling of
 * {@link OmsAeIntegrationTest}, which keeps covering the feed-off legacy path unchanged.
 *
 * <p>What is proven, and how the negatives stay race-free (no timed waits — the idiom of the assets
 * repo's RequestOriginEgressClusterTest):</p>
 * <ol>
 *   <li><b>Snapshot bootstrap lands despite the narrowed mask</b> — replies to a session's own
 *       commands are origin-routed and mask-exempt, so {@code isProjectionReady()} flips even
 *       though the mask holds neither {@code balances} nor {@code snapshots}.</li>
 *   <li><b>The firehose is off</b> — another session's deposits produce NO BalanceUpdate on this
 *       session's cluster egress. Counted at the dispatch layer
 *       ({@link AssetsClusterClient#getEgressBalanceUpdateEntries()}); the negative rides ordered
 *       fences: same-session ingress order (a fence deposit's ack proves the Subscribe before it
 *       was applied) and per-session egress FIFO (once a later fence's ack arrived, an earlier
 *       leaked broadcast would already have been dispatched and counted).</li>
 *   <li><b>The projection still converges — via the feed</b> — for foreign deposits and for the
 *       full settle flow. Each key is awaited independently: the conflated feed guarantees final
 *       values, not cross-key order.</li>
 *   <li><b>Hold acks still flow</b> — {@code store.hold()} blocking on its origin-routed HoldAck is
 *       the money path itself exercised against the narrowed session.</li>
 * </ol>
 *
 * <p>The feed channel is UDP loopback, not {@code aeron:ipc}: the production client owns an
 * embedded media driver and IPC cannot span two drivers (the AE publishes from its own). The AE
 * side is wired exactly as the assets repo's BalanceFeedClusterTest does — the
 * {@code balance.feed.channel} property read by {@link BalanceFeedRuntime#createIfEnabled}, the
 * store armed on the service before launch, the publisher started once the driver is up.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OmsAeBalanceFeedIntegrationTest {

    // ---- AE topology (disjoint from OmsAeIntegrationTest's 19500s) ----
    private static final int AE_PORT_BASE = 19800;
    private static final String INGRESS_ENDPOINTS = "0=localhost:" + (AE_PORT_BASE + 2); // 19802
    private static final String STORE_EGRESS_CHANNEL = "aeron:udp?endpoint=localhost:19894";
    private static final String RAW_EGRESS_ENDPOINT = "localhost:19895";
    private static final String FEED_CHANNEL = "aeron:udp?endpoint=localhost:19896";

    private static final int MARKET_BTC_USD = 1;
    private static final int USD = Asset.USD.id();
    private static final int BTC = Asset.BTC.id();

    private static final long CONNECT_TIMEOUT_MS = 20_000;
    private static final long SETTLE_TIMEOUT_MS = 15_000;

    /** One user only ever deposits from THIS session, as the ordered egress fence. */
    private static final long FENCE_USER = 2999L;
    /** Hammered at boot until the feed pipe is proven live; no case asserts its balance. */
    private static final long PRIMER_USER = 2998L;

    private File tmp;
    private ClusteredMediaDriver aeDriver;
    private ClusteredServiceContainer aeContainer;
    private BalanceFeedRuntime feedRuntime;

    private AssetsClusterClient client;
    private AeronAssetsBalanceStore store;
    private Thread pollThread;
    private RawClient raw;

    private final AtomicInteger journalPos = new AtomicInteger(1_000);

    // ==================== lifecycle ====================

    @BeforeAll
    void boot() {
        tmp = new File(System.getProperty("java.io.tmpdir"), "oms-ae-feed-e2e-" + AE_PORT_BASE);
        IoUtil.delete(tmp, true);
        final String aeronDir = new File(tmp, "ae-driver").getAbsolutePath();

        // Real single-node AE cluster — OmsAeIntegrationTest's boot recipe...
        final AssetsClusteredService service = new AssetsClusteredService();
        final ClusterConfig cfg = ClusterConfig.create(0, List.of("localhost"), AE_PORT_BASE, service);
        cfg.baseDir(new File(tmp, "node0"));
        cfg.aeronDirectoryName(aeronDir);
        cfg.idleStrategySupplier(BackoffIdleStrategy::new);
        cfg.errorHandler(Throwable::printStackTrace);
        cfg.consensusModuleContext()
                .ingressChannel("aeron:udp?term-length=4m")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));

        // ...plus the feed, wired the way assets' BalanceFeedClusterTest (and the AE node main)
        // does: property -> createIfEnabled, store on the service BEFORE launch, publisher started
        // once the driver is up.
        System.setProperty("balance.feed.channel", FEED_CHANNEL);
        feedRuntime = BalanceFeedRuntime.createIfEnabled(0);
        assertNotNull(feedRuntime, "balance.feed.channel is set: the feed must be ON");
        service.setBalanceFeed(feedRuntime.store());

        aeDriver = ClusteredMediaDriver.launch(
                cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true),
                cfg.archiveContext(),
                cfg.consensusModuleContext());
        feedRuntime.start(aeronDir, Throwable::printStackTrace);
        aeContainer = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());

        // Production client with the feed CONFIGURED; store unchanged (it cannot tell transports apart).
        client = new AssetsClusterClient(INGRESS_ENDPOINTS, STORE_EGRESS_CHANNEL,
                FEED_CHANNEL, AssetsClusterClient.DEFAULT_BALANCE_FEED_STREAM_ID);
        store = new AeronAssetsBalanceStore(client, Asset.count(), 5_000, 5_000);
        store.initSettleHighWater(0L);

        client.connect();
        pollThread = new Thread(client::startPolling, "oms-assets-poll");
        pollThread.setDaemon(true);
        pollThread.start();

        // (d) Bootstrap lands despite the narrowed session: the mask excludes balances AND
        // snapshots, yet the projection readies — origin replies bypass the gate (assets#56).
        awaitTrue("store projection ready", store::isProjectionReady, CONNECT_TIMEOUT_MS);
        // The Subscribe itself must be accepted before any firehose negative is measured.
        awaitTrue("Subscribe accepted by the AE",
                () -> !client.isSubscribePending() && client.getSubscribesSent() >= 1,
                CONNECT_TIMEOUT_MS);

        // Second session (CH_ALL, never subscribes): the foreign-deposit source and the settle feed.
        raw = new RawClient(aeronDir, RAW_EGRESS_ENDPOINT);

        // Prime the feed pipe (the assets repo's BalanceFeedClusterTest idiom): a frame offered
        // before the subscriber links is conflated away BY CONTRACT, and a key that never mutates
        // again would then never be re-published. Re-deposit a throwaway user until one of its
        // frames lands — each deposit re-marks the slot dirty, so this converges the moment the
        // pipe is up, with no timed sleeps. Every case after this may rely on a live pipe.
        int primerSends = 0;
        final long primerDeadline = System.currentTimeMillis() + CONNECT_TIMEOUT_MS;
        while (client.getFeedEntriesReceived() == 0) {
            assertTrue(System.currentTimeMillis() < primerDeadline, "timed out priming the balance feed");
            raw.depositAndAwaitAck(0x900L + primerSends, PRIMER_USER, USD, fp(1));
            primerSends++;
            final long spinUntil = System.currentTimeMillis() + 100;
            while (client.getFeedEntriesReceived() == 0 && System.currentTimeMillis() < spinUntil) {
                Thread.onSpinWait(); // the client's own poll thread is doing the feed work
            }
        }
    }

    @AfterAll
    void shutdown() {
        System.clearProperty("balance.feed.channel");
        if (client != null) {
            client.stopPolling();
        }
        if (pollThread != null) {
            try {
                pollThread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        CloseHelper.quietCloseAll(raw, client, aeContainer, feedRuntime, aeDriver);
        if (tmp != null) {
            IoUtil.delete(tmp, true);
        }
    }

    // ==================== cases ====================

    /** 1. The narrowed session bootstrapped: snapshot reply (origin, mask-exempt) + Subscribe live. */
    @Test
    @Order(1)
    void bootstrapLandsDespiteNarrowedSession() {
        assertTrue(store.isProjectionReady(), "snapshot reply must land despite the narrowed mask");
        assertTrue(client.getSubscribesSent() >= 1, "the session must have narrowed itself");
        assertFalse(client.isSubscribePending());
    }

    /**
     * 2. The point of the PR: a foreign session's deposit no longer rides this session's cluster
     *    egress, yet the projection converges via the feed.
     */
    @Test
    @Order(2)
    void foreignDepositsStayOffClusterEgressButConvergeViaFeed() {
        final long otherUser = 2001L;
        final long amount = fp(777);

        // Fence #1 (same-session order): this deposit was submitted AFTER the Subscribe was
        // accepted on this session, so its ack proves the engine has applied the narrowing.
        store.deposit(FENCE_USER, USD, fp(1));
        final long baseline = client.getEgressBalanceUpdateEntries();

        // Foreign deposit, acked to ITS OWN session (origin-routed) — in the log from here on.
        raw.depositAndAwaitAck(0xD1L, otherUser, USD, amount);

        // (b) The projection converges without the firehose: the only balance transport left for a
        // foreign user is the feed.
        awaitTrue("foreign deposit visible via the feed",
                () -> store.getAvailable(otherUser, USD) == amount, SETTLE_TIMEOUT_MS);
        assertTrue(client.getFeedEntriesReceived() >= 1, "convergence must have come off the feed");

        // (a) Ordered negative, no timers: the engine processed the foreign deposit before this
        // fence (its ack was awaited above), so a leaked broadcast would sit BEFORE the fence ack
        // in this session's FIFO egress — dispatched (and counted) before deposit() returned.
        store.deposit(FENCE_USER, USD, fp(1));
        assertEquals(baseline, client.getEgressBalanceUpdateEntries(),
                "a foreign live BalanceUpdate leaked onto the narrowed cluster session");
    }

    /** 3. (c) Hold acks still flow origin-routed through the narrowed session — the money gate. */
    @Test
    @Order(3)
    void holdAcksStillFlowOnTheNarrowedSession() {
        final long user = 2002L;
        store.deposit(user, USD, fp(100_000));

        assertTrue(store.hold(user, USD, fp(40_000), 30_001L),
                "hold() blocks on its origin-routed HoldAck; false would mean acks broke");
        // AWAITED, not instantaneous: the ack path applies the hold delta immediately, but a
        // conflated feed frame read BEFORE the hold (carrying available=100k/locked=0) may land
        // after the ack and stand briefly — the documented LWW window. The contract is the FINAL
        // value: the hold re-dirtied the slot, so the feed's last word for this key is 60k/40k.
        awaitTrue("hold converged in the projection",
                () -> store.getLocked(user, USD) == fp(40_000)
                        && store.getAvailable(user, USD) == fp(60_000), SETTLE_TIMEOUT_MS);
    }

    /**
     * 4. Settlement: the 4 balance lines that used to be the firehose's money case arrive via the
     *    feed; the cluster session carries none of them.
     */
    @Test
    @Order(4)
    void settlementBalancesArriveViaFeedOnly() {
        final long buyer = 2003L, seller = 2004L;
        final long buyOrder = 40_001L, sellOrder = 40_002L;
        final long tradeId = 9_001L;

        store.deposit(buyer, USD, fp(60_000));
        store.deposit(seller, BTC, fp(1.0));
        assertTrue(store.hold(buyer, USD, fp(60_000), buyOrder));
        assertTrue(store.hold(seller, BTC, fp(1.0), sellOrder));
        final long baseline = client.getEgressBalanceUpdateEntries();

        raw.settle(tradeId, MARKET_BTC_USD, buyOrder, buyer, sellOrder, seller,
                fp(60_000), fp(1.0), true, journalPos.incrementAndGet());

        // Await EACH key independently: the conflated feed promises final values, not cross-key order.
        awaitTrue("seller received quote", () -> store.getAvailable(seller, USD) == fp(60_000), SETTLE_TIMEOUT_MS);
        awaitTrue("buyer received base", () -> store.getAvailable(buyer, BTC) == fp(1.0), SETTLE_TIMEOUT_MS);
        awaitTrue("buyer hold consumed", () -> store.getLocked(buyer, USD) == 0L, SETTLE_TIMEOUT_MS);
        awaitTrue("seller hold consumed", () -> store.getLocked(seller, BTC) == 0L, SETTLE_TIMEOUT_MS);
        assertEquals(0L, store.getAvailable(buyer, USD), "buy hold fully drawn");
        assertEquals(0L, store.getAvailable(seller, BTC));

        // Ordered negative again: the feed showing the settle's effects proves the engine processed
        // it (any leak is already enqueued per-session); the fence ack then flushes this session.
        store.deposit(FENCE_USER, USD, fp(1));
        assertEquals(baseline, client.getEgressBalanceUpdateEntries(),
                "settlement BalanceUpdates must ride the feed, not the narrowed cluster session");
    }

    // ==================== helpers ====================

    private static long fp(double value) {
        return FixedPoint.fromDouble(value);
    }

    /**
     * Deadline-bounded spin on a projection condition (the client's poll thread does the real work).
     * Also services the RAW session while waiting: an AeronCluster session with no keep-alives is
     * closed server-side after ~10s, after which its offers are accepted onto the shared ingress
     * publication but silently DISCARDED at the consensus module — a later settle would vanish.
     * Both clients run on this test thread, so pumping here is single-threaded and safe.
     */
    private void awaitTrue(String what, BooleanSupplier cond, long timeoutMs) {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (!cond.getAsBoolean()) {
            assertTrue(System.currentTimeMillis() < deadline, "timed out waiting for: " + what);
            if (raw != null) {
                raw.pump();
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Second AE session on the AE's own driver (mirrors OmsAeIntegrationTest.RawFeedClient):
     * CH_ALL — it never subscribes — so it stands in for "everyone else" whose traffic used to
     * multiply onto the OMS session. Offers Deposit (awaiting its own origin-routed ack, the log
     * fence) and Settle. Single-threaded by construction: every cluster op runs on the test thread.
     */
    private static final class RawClient implements EgressListener, AutoCloseable {
        private final AeronCluster cluster;

        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final DepositAckDecoder depositAck = new DepositAckDecoder();

        private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();
        private final DepositEncoder depositEnc = new DepositEncoder();
        private final SettleEncoder settleEnc = new SettleEncoder();
        private final UnsafeBuffer buf = new UnsafeBuffer(new byte[128]);

        private final AtomicLong lastDepositAckCorr = new AtomicLong(Long.MIN_VALUE);

        RawClient(String aeronDir, String egressEndpoint) {
            final class Holder implements EgressListener {
                volatile EgressListener delegate;

                @Override
                public void onMessage(long sid, long ts, DirectBuffer b, int o, int l, Header h) {
                    final EgressListener d = delegate;
                    if (d != null) {
                        d.onMessage(sid, ts, b, o, l, h);
                    }
                }
            }
            final Holder holder = new Holder();
            this.cluster = AeronCluster.connect(new AeronCluster.Context()
                    .aeronDirectoryName(aeronDir)
                    .ingressChannel("aeron:udp?term-length=4m")
                    .ingressEndpoints(INGRESS_ENDPOINTS)
                    .egressChannel("aeron:udp?endpoint=" + egressEndpoint + "|term-length=4m")
                    .egressListener(holder));
            holder.delegate = this;
        }

        @Override
        public void onMessage(long sessionId, long timestamp, DirectBuffer buffer,
                              int offset, int length, Header h) {
            if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
                return;
            }
            header.wrap(buffer, offset);
            if (header.templateId() == DepositAckDecoder.TEMPLATE_ID) {
                depositAck.wrapAndApplyHeader(buffer, offset, header);
                lastDepositAckCorr.set(depositAck.correlationId());
            }
        }

        /** Deposit and block until ITS ack arrives — proof the deposit is applied in the log. */
        void depositAndAwaitAck(long correlationId, long userId, int assetId, long amount) {
            depositEnc.wrapAndApplyHeader(buf, 0, headerEnc)
                    .correlationId(correlationId).userId(userId).assetId(assetId).amount(amount);
            offer(MessageHeaderEncoder.ENCODED_LENGTH + depositEnc.encodedLength());
            await(() -> lastDepositAckCorr.get() == correlationId);
        }

        void settle(long tradeId, int marketId, long takerOrderId, long takerUserId,
                    long makerOrderId, long makerUserId, long price, long qty,
                    boolean takerIsBuy, long journalPosition) {
            settleEnc.wrapAndApplyHeader(buf, 0, headerEnc)
                    .tradeId(tradeId).marketId(marketId)
                    .takerOrderId(takerOrderId).takerUserId(takerUserId)
                    .makerOrderId(makerOrderId).makerUserId(makerUserId)
                    .price(price).quantity(qty)
                    .takerIsBuy(takerIsBuy ? BoolFlag.TRUE : BoolFlag.FALSE)
                    .journalPosition(journalPosition);
            offer(MessageHeaderEncoder.ENCODED_LENGTH + settleEnc.encodedLength());
        }

        /** Service this session (egress + keep-alive) without waiting on anything. */
        void pump() {
            cluster.pollEgress();
            cluster.sendKeepAlive();
        }

        private void offer(int length) {
            final long deadline = System.currentTimeMillis() + SETTLE_TIMEOUT_MS;
            while (cluster.offer(buf, 0, length) < 0) {
                assertTrue(System.currentTimeMillis() < deadline, "raw offer back-pressured too long");
                cluster.pollEgress();
                Thread.onSpinWait();
            }
        }

        private void await(BooleanSupplier cond) {
            final long deadline = System.currentTimeMillis() + SETTLE_TIMEOUT_MS;
            while (!cond.getAsBoolean()) {
                assertTrue(System.currentTimeMillis() < deadline, "raw await timed out");
                cluster.pollEgress();
                cluster.sendKeepAlive();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void close() {
            CloseHelper.quietClose(cluster);
        }
    }
}
