// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.match.infrastructure.journal.generated.*;
import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.*;
import com.openexchange.oms.core.OmsCoreEngine;
import com.openexchange.oms.core.OrderLifecycleManager;
import com.openexchange.oms.core.SyntheticOrderEngine;
import com.openexchange.oms.persistence.JournalConsumerStore;
import com.openexchange.oms.persistence.JournalConsumerStore.*;
import com.openexchange.oms.persistence.PersistenceException;
import com.openexchange.oms.risk.RiskEngine;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class JournalOutcomeConsumerTest {
    private static final String SOURCE = "me0-gen1";
    private static final long QTY = 100_000_000L;

    /** Projector tables + OMS commit, with the repository's checkpoint rules. */
    static class FakeStore implements JournalConsumerStore {
        final TreeMap<Long, Event> events = new TreeMap<>();
        Optional<Checkpoint> projector = Optional.empty();
        long targetAhead;          // recorded journal beyond the projector's cursor
        boolean projectorFresh = true;
        Optional<Checkpoint> checkpoint = Optional.empty();
        final Map<PositionKey, Long> positions = new HashMap<>();
        final List<String> log = new ArrayList<>();
        final List<Map<Long, Long>> committedFills = new ArrayList<>();
        boolean failCommit;
        boolean orders;
        @Override public Optional<Checkpoint> loadCheckpoint() { return checkpoint; }
        @Override public Optional<ProjectorStatus> projectorStatus() {
            return projector.map(cp -> new ProjectorStatus(cp, cp.position() + targetAhead, projectorFresh));
        }
        @Override public List<Event> eventsAfter(String source, long position, int limit) {
            assertEquals(projector.orElseThrow().source(), source);
            return events.tailMap(position, false).values().stream()
                    .filter(e -> e.position() <= projector.orElseThrow().position()).limit(limit).toList();
        }
        @Override public Map<PositionKey, Long> loadPositions() { return Map.copyOf(positions); }
        @Override public boolean hasOrders() { return orders; }
        @Override public void commit(Collection<OmsOrder> touched, Map<PositionKey, Long> deltas,
                                     Optional<Checkpoint> expected, Checkpoint next) {
            if (failCommit) throw new PersistenceException("injected commit failure");
            assertEquals(checkpoint, expected, "checkpoint compare-and-set");
            Map<Long, Long> fills = new TreeMap<>();
            for (OmsOrder o : touched) { fills.put(o.getOmsOrderId(), o.getFilledQty()); o.setStateRevision(o.getStateRevision() + 1); }
            committedFills.add(fills);
            deltas.forEach((k, v) -> positions.merge(k, v, Long::sum));
            checkpoint = Optional.of(next);
            log.add("commit@" + next.position());
        }
        void trade(long position, long tradeId, long takerOid, long takerOms, long makerOid, long makerOms, long qty) {
            var buffer = new UnsafeBuffer(new byte[128]);
            var e = new JournalTradeEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
            e.egressSeq(position).tradeId(tradeId).marketId(1).takerOrderId(takerOid).takerUserId(7)
                    .makerOrderId(makerOid).makerUserId(8).price(100_000_000L).quantity(qty)
                    .takerIsBuy(BooleanType.TRUE).timestamp(1).takerOmsOrderId(takerOms).makerOmsOrderId(makerOms);
            add(position, JournalTradeEncoder.TEMPLATE_ID, buffer, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
        }
        void terminal(long position, long clusterOrderId, long omsOrderId, TerminalStatus status) {
            var buffer = new UnsafeBuffer(new byte[128]);
            var e = new JournalTerminalEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
            e.egressSeq(position).orderId(clusterOrderId).userId(7).marketId(1).status(status).timestamp(1)
                    .omsOrderId(omsOrderId);
            add(position, JournalTerminalEncoder.TEMPLATE_ID, buffer, MessageHeaderEncoder.ENCODED_LENGTH + e.encodedLength());
        }
        void add(long position, int template, UnsafeBuffer buffer, int length) {
            byte[] bytes = new byte[length]; buffer.getBytes(0, bytes);
            events.put(position, new Event(position, template, bytes));
            projector = Optional.of(new Checkpoint(SOURCE, position, 0));
        }
    }

    private FakeStore store;
    private OrderLifecycleManager lcm;
    private OmsCoreEngine core;
    private RiskEngine risk;
    private int halts;
    private final List<Long> refills = new ArrayList<>();
    private JournalOutcomeConsumer consumer;

    @BeforeEach
    void setUp() {
        store = new FakeStore();
        lcm = new OrderLifecycleManager();
        core = new OmsCoreEngine(lcm, new SyntheticOrderEngine());
        core.setJournalAuthoritative(true);
        core.setClusterSubmitHandler(new OmsCoreEngine.ClusterSubmitHandler() {
            @Override public boolean submitTriggeredOrder(OmsOrder parent, OmsOrderType type, long price) { return true; }
            @Override public boolean submitIcebergSlice(OmsOrder iceberg, long qty) {
                store.log.add("refill:" + iceberg.getOmsOrderId()); refills.add(qty); return true;
            }
            @Override public void submitCancel(long cid, long user, int market) { }
            @Override public void submitOpenOrdersSnapshotRequest(long requestId) { }
        });
        risk = new RiskEngine(6, new OmsMarketDataProvider(), new OmsBalanceChecker(new com.openexchange.oms.ledger.InMemoryBalanceStore()));
        consumer = new JournalOutcomeConsumer(store, core, risk, () -> halts++);
    }

    private OmsOrder live(long id, long clusterOrderId, OrderSide side, OmsOrderType type) {
        var o = new OmsOrder();
        o.setOmsOrderId(id); o.setUserId(side == OrderSide.BUY ? 7 : 8); o.setMarketId(1);
        o.setSide(side); o.setOrderType(type); o.setTimeInForce(TimeInForce.GTC);
        o.setStatus(OmsOrderStatus.NEW); o.setStateRevision(1);
        o.setPrice(100_000_000L); o.setQuantity(QTY); o.setRemainingQty(QTY); o.setClusterOrderId(clusterOrderId);
        lcm.restoreOrder(o);
        return o;
    }

    @Test
    void appliesFillsAndTerminalsInJournalOrderWithOneCommit() {
        var buy = live(1, 11, OrderSide.BUY, OmsOrderType.LIMIT);
        var sell = live(2, 12, OrderSide.SELL, OmsOrderType.LIMIT);
        store.trade(32, 1, 11, 1, 12, 2, QTY / 4);
        store.terminal(64, 11, 1, TerminalStatus.CANCELLED);
        consumer.start();
        consumer.run();
        assertEquals(QTY / 4, buy.getFilledQty());
        assertEquals(OmsOrderStatus.CANCELLED, buy.getStatus());
        assertEquals(QTY / 4, sell.getFilledQty());
        assertEquals(OmsOrderStatus.PARTIALLY_FILLED, sell.getStatus());
        assertEquals(Optional.of(new Checkpoint(SOURCE, 64, 1)), store.checkpoint);
        assertEquals(List.of(Map.of(1L, QTY / 4, 2L, QTY / 4)), store.committedFills);
        assertEquals(Map.of(new PositionKey(7, 1), QTY / 4, new PositionKey(8, 1), -QTY / 4), store.positions);
        assertEquals(QTY / 4, risk.getPosition(7, 1));
        assertTrue(consumer.isCaughtUp());
    }

    @Test
    void repeatedTradeAtALaterPositionIsNotAppliedTwice() {
        var buy = live(1, 11, OrderSide.BUY, OmsOrderType.LIMIT);
        live(2, 12, OrderSide.SELL, OmsOrderType.LIMIT);
        store.trade(32, 1, 11, 1, 12, 2, QTY / 4);
        store.trade(96, 1, 11, 1, 12, 2, QTY / 4);
        consumer.start();
        consumer.run();
        assertEquals(QTY / 4, buy.getFilledQty());
        assertEquals(Optional.of(new Checkpoint(SOURCE, 96, 1)), store.checkpoint);
        assertNull(consumer.failure());
    }

    @Test
    void tradeGapStopsBeforeApplyingAnythingPastIt() {
        var buy = live(1, 11, OrderSide.BUY, OmsOrderType.LIMIT);
        live(2, 12, OrderSide.SELL, OmsOrderType.LIMIT);
        store.trade(32, 1, 11, 1, 12, 2, QTY / 4);
        store.trade(64, 3, 11, 1, 12, 2, QTY / 4);
        consumer.start();
        consumer.run();
        assertEquals(QTY / 4, buy.getFilledQty());
        assertEquals(Optional.of(new Checkpoint(SOURCE, 32, 1)), store.checkpoint);
        assertNotNull(consumer.failure());
        assertFalse(consumer.isCaughtUp());
        consumer.run();
        assertEquals(QTY / 4, buy.getFilledQty(), "a failed consumer makes no further progress");
    }

    @Test
    void commitFailureHaltsWithoutPostCommitSideEffects() {
        var iceberg = live(1, 11, OrderSide.BUY, OmsOrderType.ICEBERG);
        iceberg.setDisplayQuantity(QTY / 4); iceberg.setHiddenQuantity(QTY); iceberg.setSliceRemainingQty(QTY / 4);
        core.getSyntheticEngine().registerOrder(iceberg);
        live(2, 12, OrderSide.SELL, OmsOrderType.LIMIT);
        store.trade(32, 1, 11, 1, 12, 2, QTY / 4);
        store.failCommit = true;
        consumer.start();
        consumer.run();
        assertEquals(1, halts);
        assertTrue(refills.isEmpty(), "no order may be sent for an uncommitted fill");
        assertNotNull(consumer.failure());
    }

    @Test
    void icebergRefillIsSentOnlyAfterTheFillCommits() {
        var iceberg = live(1, 11, OrderSide.BUY, OmsOrderType.ICEBERG);
        iceberg.setDisplayQuantity(QTY / 4); iceberg.setHiddenQuantity(QTY); iceberg.setSliceRemainingQty(QTY / 4);
        core.getSyntheticEngine().registerOrder(iceberg);
        live(2, 12, OrderSide.SELL, OmsOrderType.LIMIT);
        store.trade(32, 1, 11, 1, 12, 2, QTY / 4);
        consumer.start();
        consumer.run();
        assertEquals(List.of("commit@32", "refill:1"), store.log);
        assertEquals(List.of(QTY / 4), refills);
    }

    @Test
    void egressTradesAreIgnoredInJournalMode() {
        var buy = live(1, 11, OrderSide.BUY, OmsOrderType.LIMIT);
        core.onTradeExecution(1, 1, 11, 12, 7, 8, 100_000_000L, QTY / 4, true, 1, 2, 5);
        assertEquals(0, buy.getFilledQty());
    }

    @Test
    void caughtUpRequiresTheProjectorPositionAndAFreshObservation() {
        live(1, 11, OrderSide.BUY, OmsOrderType.LIMIT);
        live(2, 12, OrderSide.SELL, OmsOrderType.LIMIT);
        consumer.start();
        consumer.run();
        assertFalse(consumer.isCaughtUp(), "no projector checkpoint yet");
        store.trade(32, 1, 11, 1, 12, 2, QTY / 4);
        assertFalse(consumer.isCaughtUp());
        consumer.run();
        assertTrue(consumer.isCaughtUp());
        store.projector = Optional.of(new Checkpoint("me1-gen2", 64, 1));
        consumer.run();
        assertNotNull(consumer.failure(), "a different projector source is never followed");
        assertFalse(consumer.isCaughtUp());
    }

    @Test
    void missingCheckpointIsFreshOnlyWithoutOrders() {
        consumer.start();
        assertNull(consumer.failure());
        store.orders = true;
        var restarted = new JournalOutcomeConsumer(store, core, risk, () -> halts++);
        assertThrows(IllegalStateException.class, restarted::start);
    }

    @Test
    void restartResumesAfterTheCommittedTradeWithoutReapplying() {
        var buy = live(1, 11, OrderSide.BUY, OmsOrderType.LIMIT);
        live(2, 12, OrderSide.SELL, OmsOrderType.LIMIT);
        store.trade(32, 1, 11, 1, 12, 2, QTY / 4);
        consumer.start();
        consumer.run();
        store.trade(64, 1, 11, 1, 12, 2, QTY / 4); // journal re-delivery after a producer restart
        store.trade(96, 2, 11, 1, 12, 2, QTY / 4);
        var restarted = new JournalOutcomeConsumer(store, core, risk, () -> halts++);
        restarted.start();
        restarted.run();
        assertEquals(QTY / 2, buy.getFilledQty());
        assertEquals(Optional.of(new Checkpoint(SOURCE, 96, 2)), store.checkpoint);
    }

    @Test
    void staleProjectorObservationClosesTheBarrier() {
        live(1, 11, OrderSide.BUY, OmsOrderType.LIMIT);
        live(2, 12, OrderSide.SELL, OmsOrderType.LIMIT);
        store.trade(32, 1, 11, 1, 12, 2, QTY / 4);
        consumer.start();
        consumer.run();
        assertTrue(consumer.isCaughtUp());
        store.projectorFresh = false;
        consumer.run();
        assertFalse(consumer.isCaughtUp(), "a projector that stopped observing may be arbitrarily behind");
        assertNull(consumer.failure(), "staleness is not a failure; it recovers when the projector does");
        store.projectorFresh = true;
        consumer.run();
        assertTrue(consumer.isCaughtUp());
    }

    @Test
    void projectorLagBeyondTheBoundClosesTheBarrier() {
        live(1, 11, OrderSide.BUY, OmsOrderType.LIMIT);
        live(2, 12, OrderSide.SELL, OmsOrderType.LIMIT);
        store.trade(32, 1, 11, 1, 12, 2, QTY / 4);
        store.targetAhead = JournalOutcomeConsumer.MAX_LAG_BYTES + 32;
        consumer.start();
        consumer.run();
        assertFalse(consumer.isCaughtUp());
        store.targetAhead = JournalOutcomeConsumer.MAX_LAG_BYTES;
        consumer.run();
        assertTrue(consumer.isCaughtUp(), "bounded lag under load keeps admission open");
    }

    @Test
    void consumerWithinTheBoundStaysCaughtUpUnderLoad() {
        live(1, 11, OrderSide.BUY, OmsOrderType.LIMIT);
        live(2, 12, OrderSide.SELL, OmsOrderType.LIMIT);
        for (int n = 1; n <= JournalOutcomeConsumer.BATCH + 10; n++) store.trade(32L * n, n, 11, 1, 12, 2, 1);
        consumer.start();
        consumer.run(); // one bounded batch; 10 events remain
        assertTrue(consumer.isCaughtUp(), "admission must not flap while the consumer is a batch behind");
        assertEquals(32L * JournalOutcomeConsumer.BATCH, consumer.checkpoint().orElseThrow().position());
    }
}
