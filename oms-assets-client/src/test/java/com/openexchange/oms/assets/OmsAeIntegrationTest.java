// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

import com.match.domain.FixedPoint;
import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEndDecoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEntryDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.RequestHoldSnapshotEncoder;
import com.openexchange.assets.infrastructure.generated.SettleEncoder;
import com.openexchange.assets.infrastructure.generated.SettlementAppliedDecoder;
import com.openexchange.assets.infrastructure.generated.TerminalReleaseEncoder;
import com.openexchange.assets.infrastructure.persistence.AssetsClusteredService;
import com.openexchange.assets.infrastructure.persistence.ClusterConfig;
import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.domain.SnowflakeIdGenerator;
import com.openexchange.oms.common.enums.Asset;
import com.openexchange.oms.common.enums.OmsOrderType;
import com.openexchange.oms.common.enums.OrderSide;
import com.openexchange.oms.common.enums.TimeInForce;
import com.openexchange.oms.ledger.LedgerEntry;
import com.openexchange.oms.ledger.LedgerService;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OMS↔Assets-Engine integration test: the REAL {@link AssetsClusterClient} + {@link AeronAssetsBalanceStore}
 * + {@link LedgerService}, driven with real {@link OmsOrder} objects, against a REAL in-process single-node
 * AE cluster (embedded {@link ClusteredMediaDriver} + {@link ClusteredServiceContainer}, backoff idle,
 * isolated ports/dirs). Every money movement is asserted through the store's own balance projection; the
 * synthetic-parent case additionally decodes the AE's authoritative hold snapshot on a raw client.
 *
 * <p>Two client sessions share the one AE. The <b>store</b> owns a session over the production
 * {@link AssetsClusterClient} (its own embedded media driver, single poll thread); the <b>raw feed
 * client</b> ({@link RawFeedClient}) owns a second session on the AE's own driver and stands in for the
 * matching-engine settlement feed — it offers {@code Settle}/{@code TerminalRelease} ingress and reads
 * {@code RequestHoldSnapshot} answers directly off the wire, mirroring assets-bridge's
 * {@code BridgeEndToEndTest.TestAeClient}. The AE's egress is broadcast to <em>all</em> sessions
 * (AssetsClusteredService#broadcast), so a settle fed on the raw session lands in the store's projection.</p>
 *
 * <p>Cases are ordered and share one cluster boot; each uses a distinct user/order so state from an
 * earlier case cannot mask a later assertion. Awaits are deadline-bounded (mirror BridgeEndToEndTest).</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OmsAeIntegrationTest {

    // ---- AE topology (isolated from the live stack and from assets-bridge's 19400s) ----
    private static final int AE_PORT_BASE = 19500;
    // clientFacingPort = portBase + nodeId*100 + 2 (ClusterConfig.CLIENT_FACING_PORT_OFFSET).
    private static final String INGRESS_ENDPOINTS = "0=localhost:" + (AE_PORT_BASE + 2); // 19502
    private static final String STORE_EGRESS_CHANNEL = "aeron:udp?endpoint=localhost:19594";
    private static final String RAW_FEED_EGRESS_ENDPOINT = "localhost:19595";

    private static final int MARKET_BTC_USD = 1;
    private static final int USD = Asset.USD.id(); // 0
    private static final int BTC = Asset.BTC.id(); // 1

    private static final long CONNECT_TIMEOUT_MS = 20_000;
    private static final long SETTLE_TIMEOUT_MS = 15_000;

    private File tmp;
    private ClusteredMediaDriver aeDriver;
    private ClusteredServiceContainer aeContainer;

    private AssetsClusterClient client;
    private AeronAssetsBalanceStore store;
    private LedgerService ledger;
    private Thread pollThread;
    private RawFeedClient feed;

    private final AtomicInteger journalPos = new AtomicInteger(1_000);

    // ==================== lifecycle ====================

    @BeforeAll
    void boot() {
        tmp = new File(System.getProperty("java.io.tmpdir"), "oms-ae-e2e-" + AE_PORT_BASE);
        IoUtil.delete(tmp, true);

        // Real single-node AE cluster (embedded driver, backoff idle) — BridgeEndToEndTest recipe.
        final ClusterConfig cfg = ClusterConfig.create(0, List.of("localhost"), AE_PORT_BASE,
                new AssetsClusteredService());
        cfg.baseDir(new File(tmp, "node0"));
        cfg.aeronDirectoryName(new File(tmp, "ae-driver").getAbsolutePath());
        cfg.idleStrategySupplier(BackoffIdleStrategy::new);
        cfg.errorHandler(Throwable::printStackTrace);
        cfg.consensusModuleContext()
                .ingressChannel("aeron:udp?term-length=4m")
                .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .electionTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(2))
                .terminationTimeoutNs(TimeUnit.SECONDS.toNanos(2));
        aeDriver = ClusteredMediaDriver.launch(
                cfg.mediaDriverContext().dirDeleteOnStart(true).dirDeleteOnShutdown(true),
                cfg.archiveContext(),
                cfg.consensusModuleContext());
        aeContainer = ClusteredServiceContainer.launch(cfg.clusteredServiceContext());

        // Real production client + store + ledger, pointed at the test AE.
        client = new AssetsClusterClient(INGRESS_ENDPOINTS, STORE_EGRESS_CHANNEL);
        store = new AeronAssetsBalanceStore(client, Asset.count(), 5_000, 5_000);
        store.initSettleHighWater(0L);
        ledger = new LedgerService(store, new SnowflakeIdGenerator(0));

        client.connect();
        pollThread = new Thread(client::startPolling, "oms-assets-poll");
        pollThread.setDaemon(true);
        pollThread.start();

        // onConnected() enqueued a balance-snapshot bootstrap; wait for it to complete (virgin ledger).
        awaitTrue("store projection ready", store::isProjectionReady, CONNECT_TIMEOUT_MS);

        // Second session: the simulated ME settlement feed, on the AE's own driver.
        feed = new RawFeedClient(new File(tmp, "ae-driver").getAbsolutePath(), RAW_FEED_EGRESS_ENDPOINT);
    }

    @AfterAll
    void shutdown() {
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
        CloseHelper.quietCloseAll(feed, client, aeContainer, aeDriver);
        if (tmp != null) {
            IoUtil.delete(tmp, true);
        }
    }

    // ==================== cases ====================

    /** 1. Deposit → the store projection reflects it (projection was already awaited ready). */
    @Test
    @Order(1)
    void depositLandsInProjection() {
        final long user = 1001L;
        final long amount = fp(100_000);
        store.deposit(user, USD, amount); // synchronous: blocks on DepositAck, which sets the projection
        assertEquals(amount, store.getAvailable(user, USD), "deposit visible in projection");
        assertEquals(0L, store.getLocked(user, USD));
    }

    /** 2. holdForOrder (limit buy) → HoldAck → getAvailable/getLocked reflect it immediately (read-your-hold). */
    @Test
    @Order(2)
    void limitBuyHoldReflectsImmediately() {
        final long user = 1002L;
        store.deposit(user, USD, fp(100_000));

        final OmsOrder buy = order(20_001L, user, OrderSide.BUY, OmsOrderType.LIMIT, 50_000, 1.0);
        final List<LedgerEntry> entries = ledger.holdForOrder(buy);

        assertFalse(entries.isEmpty(), "hold should have succeeded");
        final long held = fp(50_000); // price * qty
        assertEquals(held, buy.getHoldAmount());
        // Synchronous hold: the ack path applied the projection delta before holdForOrder returned.
        assertEquals(fp(100_000) - held, store.getAvailable(user, USD));
        assertEquals(held, store.getLocked(user, USD));
    }

    /** 3. Hold rejected on insufficient funds → the order-create path (holdForOrder) sees false / empty. */
    @Test
    @Order(3)
    void insufficientFundsHoldRejected() {
        final long user = 1003L;
        store.deposit(user, USD, fp(100)); // far short of a 50k notional

        final OmsOrder buy = order(30_001L, user, OrderSide.BUY, OmsOrderType.LIMIT, 50_000, 1.0);
        final List<LedgerEntry> entries = ledger.holdForOrder(buy);

        assertTrue(entries.isEmpty(), "under-funded hold must be rejected");
        assertEquals(fp(100), store.getAvailable(user, USD), "reject moves no money");
        assertEquals(0L, store.getLocked(user, USD));
    }

    /**
     * 4. Amend GROW (holdAmendDelta, same omsOrderId) → top-up visible; amend SHRINK (releaseAmendDelta)
     *    → partial release visible. Grow is a synchronous top-up (blocks on HoldAck); shrink is a
     *    fire-and-forget Release, so its effect is awaited off the async BalanceUpdate.
     */
    @Test
    @Order(4)
    void amendGrowThenShrink() {
        final long user = 1004L;
        store.deposit(user, USD, fp(100_000));

        final OmsOrder buy = order(40_001L, user, OrderSide.BUY, OmsOrderType.LIMIT, 50_000, 1.0);
        assertFalse(ledger.holdForOrder(buy).isEmpty());
        assertEquals(fp(50_000), store.getLocked(user, USD));

        // Grow the reservation by 10k (top-up under the same orderId).
        assertFalse(ledger.holdAmendDelta(buy, fp(10_000)).isEmpty(), "grow top-up should succeed");
        assertEquals(fp(60_000), store.getLocked(user, USD), "top-up visible immediately");
        assertEquals(fp(100_000) - fp(60_000), store.getAvailable(user, USD));

        // Shrink by 20k (partial residual release — async).
        assertFalse(ledger.releaseAmendDelta(buy, fp(20_000)).isEmpty(), "shrink release should be enqueued");
        awaitTrue("amend shrink applied",
                () -> store.getLocked(user, USD) == fp(40_000), SETTLE_TIMEOUT_MS);
        assertEquals(fp(100_000) - fp(40_000), store.getAvailable(user, USD));
    }

    /**
     * 5. Settle via a direct {@code Settle} ingress (simulated feed on the raw client) with the OMS order
     *    ids + a journalPosition. The OMS-side store.settle() LOCAL dedupe returns true-then-false, and the
     *    AE actually moves both books (visible in the projection via the broadcast BalanceUpdates).
     */
    @Test
    @Order(5)
    void settleMovesBothBooksAndStoreDedupes() {
        final long buyer = 1005L, seller = 1006L;
        final long buyOrder = 50_001L, sellOrder = 50_002L;
        final long tradeId = 5_001L;

        // Fund + hold BOTH sides through the real store, and CONFIRM the holds landed (synchronous acks)
        // before feeding the settle — cross-session ingress has no ordering guarantee; production is
        // protected by the hold-before-submit gate and the test protects itself the same way.
        store.deposit(buyer, USD, fp(60_000));
        store.deposit(seller, BTC, fp(1.0));
        assertFalse(ledger.holdForOrder(order(buyOrder, buyer, OrderSide.BUY, OmsOrderType.LIMIT, 60_000, 1.0)).isEmpty());
        assertFalse(ledger.holdForOrder(order(sellOrder, seller, OrderSide.SELL, OmsOrderType.LIMIT, 60_000, 1.0)).isEmpty());
        assertEquals(fp(60_000), store.getLocked(buyer, USD));
        assertEquals(fp(1.0), store.getLocked(seller, BTC));

        // OMS-side dedupe gate (local high-water; independent of the AE feed).
        assertTrue(store.settle(buyer, seller, BTC, USD, fp(1.0), fp(60_000), tradeId), "first settle newly applied");
        assertFalse(store.settle(buyer, seller, BTC, USD, fp(1.0), fp(60_000), tradeId), "redelivery deduped");

        // Feed the trade to the AE (taker = buyer). journalPosition advances the AE consume cursor.
        feed.settle(tradeId, MARKET_BTC_USD, buyOrder, buyer, sellOrder, seller,
                fp(60_000), fp(1.0), true, journalPos.incrementAndGet());

        // The 4 balance lines are emitted seller-quote LAST; awaiting it means all 4 have applied.
        awaitTrue("seller received quote", () -> store.getAvailable(seller, USD) == fp(60_000), SETTLE_TIMEOUT_MS);
        // Buyer: hold fully consumed, base delivered.
        assertEquals(0L, store.getLocked(buyer, USD));
        assertEquals(0L, store.getAvailable(buyer, USD));
        assertEquals(fp(1.0), store.getAvailable(buyer, BTC));
        // Seller: base hold consumed, quote received.
        assertEquals(0L, store.getLocked(seller, BTC));
        assertEquals(0L, store.getAvailable(seller, BTC));
    }

    /**
     * 6. Residual model with price improvement: a buy held at 60k fills 0.5 @ 55k, then is cancelled.
     *    releaseForCancel() releases the EXACT remaining reservation (residual = holdAmount − 55k·fillQty,
     *    which includes the (60k−55k)·0.5 price improvement) — not the OMS-computed netted amount.
     */
    @Test
    @Order(6)
    void partialFillThenCancelReleasesExactResidualWithPriceImprovement() {
        final long buyer = 1007L, seller = 1008L;
        final long buyOrder = 60_001L, sellOrder = 60_002L;
        final long tradeId = 6_001L;

        store.deposit(buyer, USD, fp(60_000));
        store.deposit(seller, BTC, fp(0.5));
        final OmsOrder buy = order(buyOrder, buyer, OrderSide.BUY, OmsOrderType.LIMIT, 60_000, 1.0);
        assertFalse(ledger.holdForOrder(buy).isEmpty());
        assertFalse(ledger.holdForOrder(order(sellOrder, seller, OrderSide.SELL, OmsOrderType.LIMIT, 55_000, 0.5)).isEmpty());
        assertEquals(fp(60_000), buy.getHoldAmount());

        // Partial fill 0.5 BTC @ 55,000 (fill BELOW the 60k hold price → 2,500 price improvement).
        feed.settle(tradeId, MARKET_BTC_USD, buyOrder, buyer, sellOrder, seller,
                fp(55_000), fp(0.5), true, journalPos.incrementAndGet());
        awaitTrue("partial settle applied",
                () -> store.getAvailable(seller, USD) == fp(55_000 * 0.5), SETTLE_TIMEOUT_MS);

        final long drawn = FixedPoint.multiply(fp(55_000), fp(0.5)); // 27,500 drawn from the buy hold
        final long expectedResidual = buy.getHoldAmount() - drawn;    // 60,000 − 27,500 = 32,500
        assertEquals(expectedResidual, store.getLocked(buyer, USD), "hold residual before cancel");

        // Cancel the remainder. releaseForCancel → releaseAll (residual store) returns the FULL residual.
        buy.setFilledQty(fp(0.5));
        buy.setRemainingQty(fp(0.5));
        assertFalse(ledger.releaseForCancel(buy).isEmpty(), "cancel release should be enqueued");

        awaitTrue("residual fully released",
                () -> store.getLocked(buyer, USD) == 0L, SETTLE_TIMEOUT_MS);
        assertEquals(expectedResidual, store.getAvailable(buyer, USD),
                "released residual includes the price improvement");
    }

    /**
     * 7. Synthetic-parent release ownership. An ICEBERG hold is placed omsManagedRelease=TRUE. The AE's
     *    authoritative hold snapshot (decoded on the raw client) shows it; a FEED TerminalRelease for that
     *    omsOrderId is SUPPRESSED (locked unchanged, hold still present in the next snapshot); a CLIENT
     *    releaseAll DOES release it.
     */
    @Test
    @Order(7)
    void syntheticParentHoldSurvivesFeedTerminalButNotClientRelease() {
        final long user = 1009L;
        final long parent = 70_001L;
        final long held = fp(60_000);

        store.deposit(user, USD, held);
        final OmsOrder iceberg = order(parent, user, OrderSide.BUY, OmsOrderType.ICEBERG, 60_000, 1.0);
        assertTrue(iceberg.getOrderType().isSynthetic());
        assertFalse(ledger.holdForOrder(iceberg).isEmpty());
        assertEquals(held, store.getLocked(user, USD));

        // The AE's own hold snapshot shows the parent hold with its full remaining.
        feed.requestHoldSnapshotAndWait(1L, SETTLE_TIMEOUT_MS);
        long[] snap = feed.hold(parent);
        assertEquals(user, snap[0], "snapshot userId");
        assertEquals(USD, (int) snap[1], "snapshot assetId");
        assertEquals(held, snap[2], "snapshot remaining");

        // A FEED terminal for the parent must NOT release it (slices share the parent's omsOrderId).
        feed.terminalRelease(parent, user, journalPos.incrementAndGet(), System.currentTimeMillis());
        // Re-snapshot on the same (ordered) feed session: proves the terminal was processed AND no-op'd.
        feed.requestHoldSnapshotAndWait(2L, SETTLE_TIMEOUT_MS);
        snap = feed.hold(parent);
        assertEquals(held, snap[2], "feed terminal must not release an omsManagedRelease hold");
        assertEquals(held, store.getLocked(user, USD), "projection locked unchanged after feed terminal");

        // A CLIENT-initiated releaseAll DOES release the parent's residual.
        assertTrue(store.releaseAll(user, USD, parent));
        awaitTrue("client releaseAll applied", () -> store.getLocked(user, USD) == 0L, SETTLE_TIMEOUT_MS);
        assertEquals(held, store.getAvailable(user, USD));
    }

    /** 8. Withdraw of the remaining available works; an over-withdraw throws IllegalStateException. */
    @Test
    @Order(8)
    void withdrawThenOverWithdrawThrows() {
        final long user = 1010L;
        store.deposit(user, USD, fp(500));
        store.withdraw(user, USD, fp(200)); // synchronous WithdrawAck
        awaitTrue("withdraw applied", () -> store.getAvailable(user, USD) == fp(300), SETTLE_TIMEOUT_MS);

        store.withdraw(user, USD, fp(300)); // drain the rest
        awaitTrue("drain applied", () -> store.getAvailable(user, USD) == 0L, SETTLE_TIMEOUT_MS);

        // Over-withdraw: AE rejects (insufficient funds) → store surfaces IllegalStateException.
        assertThrows(IllegalStateException.class, () -> store.withdraw(user, USD, fp(1)));
        assertEquals(0L, store.getAvailable(user, USD), "rejected withdraw moves no money");
    }

    // ==================== helpers ====================

    private static long fp(double value) {
        return FixedPoint.fromDouble(value);
    }

    private static OmsOrder order(long omsOrderId, long userId, OrderSide side, OmsOrderType type,
                                  double price, double qty) {
        final OmsOrder o = new OmsOrder();
        o.setOmsOrderId(omsOrderId);
        o.setUserId(userId);
        o.setMarketId(MARKET_BTC_USD);
        o.setSide(side);
        o.setOrderType(type);
        o.setTimeInForce(TimeInForce.GTC);
        o.setPrice(fp(price));
        o.setQuantity(fp(qty));
        o.setRemainingQty(fp(qty));
        return o;
    }

    /** Deadline-bounded spin on a store-projection condition (the store's poll thread does the work). */
    private static void awaitTrue(String what, BooleanSupplier cond, long timeoutMs) {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (!cond.getAsBoolean()) {
            assertTrue(System.currentTimeMillis() < deadline, "timed out waiting for: " + what);
            sleepQuiet();
        }
    }

    private static void sleepQuiet() {
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * A minimal AE client that stands in for the ME settlement feed: it OFFERS {@code Settle} /
     * {@code TerminalRelease} ingress and reads {@code RequestHoldSnapshot} answers straight off the wire.
     * Single-threaded by construction — every cluster op (offer, pollEgress, sendKeepAlive) runs on the
     * calling test thread, because {@link AeronCluster} is not thread-safe (this is exactly why
     * BridgeEndToEndTest.TestAeClient polls inline rather than on a background thread).
     */
    private static final class RawFeedClient implements EgressListener, AutoCloseable {
        private final AeronCluster cluster;

        private final MessageHeaderDecoder header = new MessageHeaderDecoder();
        private final HoldSnapshotEntryDecoder holdEntry = new HoldSnapshotEntryDecoder();
        private final HoldSnapshotEndDecoder holdEnd = new HoldSnapshotEndDecoder();
        private final SettlementAppliedDecoder settlementApplied = new SettlementAppliedDecoder();

        private final MessageHeaderEncoder headerEnc = new MessageHeaderEncoder();
        private final SettleEncoder settleEnc = new SettleEncoder();
        private final TerminalReleaseEncoder terminalEnc = new TerminalReleaseEncoder();
        private final RequestHoldSnapshotEncoder reqHoldSnapEnc = new RequestHoldSnapshotEncoder();
        private final UnsafeBuffer buf = new UnsafeBuffer(new byte[128]);

        /** orderId -> {userId, assetId, remaining} for the most recently streamed hold snapshot. */
        private final Map<Long, long[]> holds = new ConcurrentHashMap<>();
        private volatile long holdSnapEndCorr = Long.MIN_VALUE;
        private final AtomicInteger settlementsApplied = new AtomicInteger();

        RawFeedClient(String aeronDir, String egressEndpoint) {
            // A delegating holder so `this` is registered only after the object is fully constructed.
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
            header.wrap(buffer, offset);
            final int templateId = header.templateId();
            if (templateId == HoldSnapshotEntryDecoder.TEMPLATE_ID) {
                holdEntry.wrapAndApplyHeader(buffer, offset, header);
                holds.put(holdEntry.orderId(),
                        new long[] {holdEntry.userId(), holdEntry.assetId(), holdEntry.remaining()});
            } else if (templateId == HoldSnapshotEndDecoder.TEMPLATE_ID) {
                holdEnd.wrapAndApplyHeader(buffer, offset, header);
                holdSnapEndCorr = holdEnd.correlationId();
            } else if (templateId == SettlementAppliedDecoder.TEMPLATE_ID) {
                settlementApplied.wrapAndApplyHeader(buffer, offset, header);
                settlementsApplied.incrementAndGet();
            }
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

        void terminalRelease(long orderId, long userId, long journalPosition, long timestamp) {
            terminalEnc.wrapAndApplyHeader(buf, 0, headerEnc)
                    .journalPosition(journalPosition).orderId(orderId).userId(userId).timestamp(timestamp);
            offer(MessageHeaderEncoder.ENCODED_LENGTH + terminalEnc.encodedLength());
        }

        /** Offer a hold-snapshot request and block until its terminator (with this correlationId) arrives. */
        void requestHoldSnapshotAndWait(long correlationId, long timeoutMs) {
            holds.clear();
            holdSnapEndCorr = Long.MIN_VALUE;
            reqHoldSnapEnc.wrapAndApplyHeader(buf, 0, headerEnc).correlationId(correlationId);
            offer(MessageHeaderEncoder.ENCODED_LENGTH + reqHoldSnapEnc.encodedLength());
            await(() -> holdSnapEndCorr == correlationId, timeoutMs);
        }

        long[] hold(long orderId) {
            final long[] v = holds.get(orderId);
            assertTrue(v != null, "expected a hold snapshot entry for order " + orderId);
            return v;
        }

        private void offer(int length) {
            final long deadline = System.currentTimeMillis() + SETTLE_TIMEOUT_MS;
            while (cluster.offer(buf, 0, length) < 0) {
                assertTrue(System.currentTimeMillis() < deadline, "raw feed offer back-pressured too long");
                cluster.pollEgress();
                Thread.onSpinWait();
            }
        }

        private void await(BooleanSupplier cond, long timeoutMs) {
            final long deadline = System.currentTimeMillis() + timeoutMs;
            while (!cond.getAsBoolean()) {
                assertTrue(System.currentTimeMillis() < deadline, "raw feed await timed out");
                cluster.pollEgress();
                cluster.sendKeepAlive();
                sleepQuiet();
            }
        }

        @Override
        public void close() {
            CloseHelper.quietClose(cluster);
        }
    }
}
