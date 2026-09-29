// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.match.infrastructure.journal.generated.BooleanType;
import com.match.infrastructure.journal.generated.JournalTerminalDecoder;
import com.match.infrastructure.journal.generated.JournalTradeDecoder;
import com.match.infrastructure.journal.generated.MessageHeaderDecoder;
import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OrderSide;
import com.openexchange.oms.core.OmsCoreEngine;
import com.openexchange.oms.persistence.JournalConsumerStore;
import com.openexchange.oms.persistence.JournalConsumerStore.Checkpoint;
import com.openexchange.oms.persistence.JournalConsumerStore.Event;
import com.openexchange.oms.persistence.JournalConsumerStore.PositionKey;
import com.openexchange.oms.risk.RiskEngine;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Applies projector-committed ME journal events to OMS live order and risk state (execution writer
 * cutover, option B: the committed journal is the only fill and terminal source).
 * <p>
 * One batch: decode and check trade density, take the touched orders' monitors in id order, apply
 * fills/terminals in journal order, then commit the order rows, position deltas and this consumer's
 * checkpoint in one transaction. Anything that sends an order (iceberg refills) runs only after the
 * commit. A failure while state is mutated but uncommitted leaves memory ahead of Postgres, so the
 * process halts and restarts from the durable checkpoint.
 */
public final class JournalOutcomeConsumer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(JournalOutcomeConsumer.class);

    static final int BATCH = 256;
    static final long FRESHNESS_NS = 3_000_000_000L;
    static final long MAX_LAG_BYTES = 1L << 20;
    private static final int JOURNAL_SCHEMA = 3, JOURNAL_VERSION = 1;
    private static final int TEMPLATE_TRADE = 1, TEMPLATE_TERMINAL = 2, TEMPLATE_COMMAND_OUTCOME = 28;

    private final JournalConsumerStore store;
    private final OmsCoreEngine coreEngine;
    private final RiskEngine riskEngine;
    private final Runnable haltAfterCommitFailure;

    private final MessageHeaderDecoder header = new MessageHeaderDecoder();
    private final JournalTradeDecoder trade = new JournalTradeDecoder();
    private final JournalTerminalDecoder terminal = new JournalTerminalDecoder();
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[0]);

    // Owned by the scheduler thread.
    private Optional<Checkpoint> checkpoint = Optional.empty();
    private boolean started;
    private volatile String failure;
    private volatile boolean caughtUp;
    private volatile long observedAtNs;

    private record Planned(long position, int template, long a, long b, long c, long d, long e, long f,
                           boolean takerIsBuy, long quantity, int marketId, int status) {}

    public JournalOutcomeConsumer(JournalConsumerStore store, OmsCoreEngine coreEngine, RiskEngine riskEngine,
                                  Runnable haltAfterCommitFailure) {
        this.store = store;
        this.coreEngine = coreEngine;
        this.riskEngine = riskEngine;
        this.haltAfterCommitFailure = haltAfterCommitFailure;
    }

    /** Load the checkpoint. Without one, orders must not exist yet: they were never journal-applied. */
    public void start() {
        checkpoint = store.loadCheckpoint();
        if (checkpoint.isEmpty() && store.hasOrders()) {
            throw new IllegalStateException("Orders exist without an OMS journal checkpoint; seed it through the writer handoff");
        }
        started = true;
        log.info("Journal consumer starting at {}", checkpoint.map(Object::toString).orElse("genesis"));
    }

    /** Admission barrier input: applied up to the projector's committed position, observed recently. */
    public boolean isCaughtUp() {
        return failure == null && caughtUp && System.nanoTime() - observedAtNs < FRESHNESS_NS;
    }

    public String failure() { return failure; }

    public Optional<Checkpoint> checkpoint() { return checkpoint; }

    @Override
    public void run() {
        if (!started || failure != null) return;
        try {
            step();
        } catch (RuntimeException e) {
            fail("journal read failed: " + e.getMessage());
            log.error("Journal consumer read failed; admission stays closed", e);
        }
    }

    private void step() {
        Optional<JournalConsumerStore.ProjectorStatus> status = store.projectorStatus();
        observedAtNs = System.nanoTime();
        if (status.isEmpty()) {
            caughtUp = false; // nothing proves the projector has covered the journal
            return;
        }
        Optional<Checkpoint> projector = Optional.of(status.get().checkpoint());
        String source = projector.get().source();
        if (checkpoint.isPresent() && !checkpoint.get().source().equals(source)) {
            fail("projector source changed from " + checkpoint.get().source() + " to " + source);
            return;
        }
        long from = checkpoint.map(Checkpoint::position).orElse(0L);
        long lastTrade = checkpoint.map(Checkpoint::lastTradeId).orElse(0L);
        List<Event> events = store.eventsAfter(source, from, BATCH);
        if (events.isEmpty()) {
            caughtUp = withinBound(status.get(), from);
            return;
        }

        List<Planned> plan = new ArrayList<>(events.size());
        long lastPosition = from;
        String stop = null;
        for (Event event : events) {
            buffer.wrap(event.payload());
            if (event.payload().length < MessageHeaderDecoder.ENCODED_LENGTH) { stop = "truncated event at " + event.position(); break; }
            header.wrap(buffer, 0);
            int template = header.templateId();
            if (template != event.templateId()) { stop = "template mismatch at " + event.position(); break; }
            if (template == TEMPLATE_COMMAND_OUTCOME) {
                lastPosition = event.position(); // resolved by the durable command dispatcher
                continue;
            }
            if (header.schemaId() != JOURNAL_SCHEMA || header.version() != JOURNAL_VERSION) {
                stop = "unsupported journal schema at " + event.position(); break;
            }
            if (template == TEMPLATE_TRADE) {
                trade.wrapAndApplyHeader(buffer, 0, header);
                long tradeId = trade.tradeId();
                if (tradeId <= lastTrade) {
                    lastPosition = event.position(); // re-delivery of an applied trade
                    continue;
                }
                if (tradeId != lastTrade + 1) { stop = "trade gap: expected " + (lastTrade + 1) + " got " + tradeId; break; }
                lastTrade = tradeId;
                plan.add(new Planned(event.position(), template, trade.takerOrderId(), trade.makerOrderId(),
                        trade.takerOmsOrderId(), trade.makerOmsOrderId(), trade.takerUserId(), trade.makerUserId(),
                        trade.takerIsBuy() == BooleanType.TRUE, trade.quantity(), trade.marketId(), 0));
            } else if (template == TEMPLATE_TERMINAL) {
                terminal.wrapAndApplyHeader(buffer, 0, header);
                plan.add(new Planned(event.position(), template, terminal.orderId(), 0, terminal.omsOrderId(), 0,
                        terminal.userId(), 0, false, 0, terminal.marketId(), terminal.statusRaw()));
            } else {
                stop = "unknown journal template " + template + " at " + event.position(); break;
            }
            lastPosition = event.position();
        }

        if (lastPosition > from) {
            Checkpoint next = new Checkpoint(source, lastPosition, lastTrade);
            if (!applyAndCommit(plan, next)) return;
        }
        if (stop != null) {
            fail(stop);
            return;
        }
        caughtUp = withinBound(status.get(), lastPosition);
    }

    private boolean applyAndCommit(List<Planned> plan, Checkpoint next) {
        var lcm = coreEngine.getLifecycleManager();
        TreeMap<Long, OmsOrder> locked = new TreeMap<>();
        for (Planned p : plan) {
            for (long id : new long[] {p.c(), p.d()}) {
                if (id == 0) continue;
                OmsOrder order = lcm.getOrder(id);
                if (order != null) locked.put(id, order);
            }
        }
        List<Long> exhausted = new ArrayList<>();
        boolean[] committed = {false};
        try {
            withMonitors(new ArrayList<>(locked.values()), 0, () -> {
                LinkedHashMap<Long, OmsOrder> touched = new LinkedHashMap<>();
                Map<PositionKey, Long> deltas = new HashMap<>();
                for (Planned p : plan) {
                    if (p.template() == TEMPLATE_TRADE) {
                        var result = coreEngine.applyJournalTrade(p.a(), p.b(), p.c(), p.d(), p.quantity());
                        result.touched().forEach(o -> touched.put(o.getOmsOrderId(), o));
                        exhausted.addAll(result.exhaustedIcebergSlices());
                        long buyer = p.takerIsBuy() ? p.e() : p.f(), seller = p.takerIsBuy() ? p.f() : p.e();
                        riskEngine.onFill(buyer, p.marketId(), OrderSide.BUY, p.quantity());
                        riskEngine.onFill(seller, p.marketId(), OrderSide.SELL, p.quantity());
                        deltas.merge(new PositionKey(buyer, p.marketId()), p.quantity(), Math::addExact);
                        deltas.merge(new PositionKey(seller, p.marketId()), -p.quantity(), Math::addExact);
                    } else if (p.c() != 0) {
                        OmsOrder ended = lcm.onJournalTerminal(p.c(), p.a(), p.status());
                        if (ended != null) touched.put(ended.getOmsOrderId(), ended);
                    }
                }
                deltas.values().removeIf(v -> v == 0);
                store.commit(touched.values(), deltas, checkpoint, next);
                committed[0] = true;
            });
        } catch (RuntimeException e) {
            fail((committed[0] ? "post-commit failure: " : "uncommitted journal batch: ") + e.getMessage());
            log.error("Journal consumer batch failed; memory may be ahead of Postgres, halting for recovery", e);
            haltAfterCommitFailure.run();
            return false;
        }
        checkpoint = Optional.of(next);
        for (long id : exhausted) {
            coreEngine.getSyntheticEngine().onIcebergSliceFilled(id);
        }
        return true;
    }

    /**
     * Admission barrier: the projector is live (database-clock freshness) and within a bounded lag of
     * the recorded journal, and this consumer is within the same bound of the projector. A bound, not
     * equality, so steady traffic does not flap admission; a stalled projector closes it.
     */
    private static boolean withinBound(JournalConsumerStore.ProjectorStatus status, long consumerPosition) {
        long projectorPosition = status.checkpoint().position();
        return status.fresh()
                && status.observedTarget() - projectorPosition <= MAX_LAG_BYTES
                && projectorPosition - consumerPosition <= MAX_LAG_BYTES;
    }

    private static void withMonitors(List<OmsOrder> orders, int index, Runnable body) {
        if (index == orders.size()) {
            body.run();
            return;
        }
        synchronized (orders.get(index)) {
            withMonitors(orders, index + 1, body);
        }
    }

    private void fail(String reason) {
        if (failure == null) failure = reason;
        caughtUp = false;
    }
}
