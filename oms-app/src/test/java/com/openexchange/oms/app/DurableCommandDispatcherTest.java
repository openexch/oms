// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.match.domain.commands.DurableOrderIntent;
import com.match.infrastructure.generated.OrderSide;
import com.match.infrastructure.generated.OrderType;
import com.openexchange.oms.cluster.OrderSubmission;
import com.openexchange.oms.common.DurableCommandWire;
import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.common.enums.OmsOrderType;
import com.openexchange.oms.core.OmsCoreEngine;
import com.openexchange.oms.core.OrderLifecycleManager;
import com.openexchange.oms.core.SyntheticOrderEngine;
import com.openexchange.oms.persistence.DurableCommandStore;
import com.openexchange.oms.persistence.PersistenceException;
import com.openexchange.oms.persistence.PostgresCommandRepository.Entry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class DurableCommandDispatcherTest {

    /** In-memory outbox with the repository's state rules. */
    static class FakeStore implements DurableCommandStore {
        final TreeMap<UUID, Entry> rows = new TreeMap<>();
        final Map<UUID, Outcome> outcomes = new HashMap<>();
        List<Entry> staleOpenPage; // simulates a page read before a concurrent abort
        @Override public Entry prepare(DurableOrderIntent i, long revision) {
            var e = rows.computeIfAbsent(i.id(), k -> new Entry(i, revision, "PREPARED"));
            if (!e.intent().equals(i)) throw new PersistenceException("conflict", null);
            return e;
        }
        @Override public void ready(DurableOrderIntent i) {
            var e = rows.get(i.id());
            if (e == null || !e.intent().equals(i) || e.state().equals("ABORTED")) throw new PersistenceException("missing", null);
            if (e.state().equals("PREPARED")) rows.put(i.id(), new Entry(i, e.revision(), "READY"));
        }
        @Override public void abortUnsent(DurableOrderIntent i) {
            var e = rows.get(i.id());
            if (e == null || !e.intent().equals(i) || e.state().equals("RESOLVED")) throw new PersistenceException("abort", null);
            rows.put(i.id(), new Entry(i, e.revision(), "ABORTED"));
        }
        @Override public List<Entry> openCommands(long h, long l, int limit) {
            if (staleOpenPage != null) { var p = staleOpenPage; staleOpenPage = null; return p; }
            var after = new UUID(h, l);
            return rows.values().stream()
                    .filter(e -> (e.state().equals("PREPARED") || e.state().equals("READY"))
                            && (h == Long.MIN_VALUE && l == Long.MIN_VALUE || e.intent().id().compareTo(after) > 0))
                    .limit(limit).toList();
        }
        @Override public List<Outcome> projectedOutcomes(int limit) {
            return rows.values().stream().filter(e -> e.state().equals("READY"))
                    .map(e -> outcomes.get(e.intent().id())).filter(Objects::nonNull)
                    .filter(o -> o.intent().equals(rows.get(o.intent().id()).intent())).limit(limit).toList();
        }
        @Override public void resolve(DurableOrderIntent i) {
            var e = rows.get(i.id());
            if (e == null || !outcomes.containsKey(i.id())) throw new PersistenceException("no outcome", null);
            if (e.state().equals("READY")) rows.put(i.id(), new Entry(i, e.revision(), "RESOLVED"));
        }
        String state(DurableOrderIntent i) { return rows.get(i.id()).state(); }
    }

    private FakeStore store;
    private OrderLifecycleManager lcm;
    private OmsCoreEngine core;
    private final List<DurableOrderIntent> sent = new ArrayList<>();
    private boolean queueAccepts = true;
    private final List<Long> persisted = new ArrayList<>();
    private DurableCommandDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        store = new FakeStore();
        lcm = new OrderLifecycleManager();
        core = new OmsCoreEngine(lcm, new SyntheticOrderEngine());
        core.setPersistenceHandler(new OmsCoreEngine.PersistenceHandler() {
            @Override public void persistOrderUpdate(OmsOrder order) { persisted.add(order.getOmsOrderId()); }
            @Override public void persistExecution(com.openexchange.oms.common.domain.ExecutionReport report) { }
        });
        dispatcher = new DurableCommandDispatcher(store, core, s -> {
            if (!queueAccepts) return false;
            assertNotNull(s.getDurableIntent(), "durable lane must never fall back to a legacy command");
            // The exact stored wire must be what the client encodes.
            assertEquals(s.getDurableIntent(), DurableCommandWire.decode(DurableCommandWire.encode(s.getDurableIntent())));
            sent.add(s.getDurableIntent());
            return true;
        });
    }

    private OmsOrder pendingNew(long omsOrderId) {
        var o = new OmsOrder();
        o.setOmsOrderId(omsOrderId); o.setUserId(100); o.setMarketId(1);
        o.setSide(com.openexchange.oms.common.enums.OrderSide.BUY); o.setOrderType(OmsOrderType.LIMIT);
        o.setPrice(1000); o.setQuantity(10); o.setRemainingQty(10);
        o.setStatus(OmsOrderStatus.PENDING_NEW); o.setStateRevision(3); o.setCreatedAtMs(System.currentTimeMillis());
        lcm.restoreOrder(o);
        return o;
    }

    private DurableOrderIntent intentFor(OmsOrder o) {
        return DurableCommandDispatcher.createIntent(o, 10_000, OrderType.LIMIT, OrderSide.BID);
    }

    private DurableOrderIntent prepared(OmsOrder o, boolean ready) {
        var i = intentFor(o);
        store.prepare(i, o.getStateRevision());
        if (ready) store.ready(i);
        return i;
    }

    @Test
    void createIdentityIsStableForTheSameWorkflowStep() {
        var o = pendingNew(9001);
        assertEquals(intentFor(o), intentFor(o));
        assertEquals(new UUID(9001, 3L << 2), intentFor(o).id());
        o.setStateRevision(4);
        assertNotEquals(new UUID(9001, 3L << 2), intentFor(o).id());
    }

    @Test
    void submitCreateReadiesTheCommandBeforeOfferingIt() {
        var o = pendingNew(9001);
        var i = intentFor(o);
        assertTrue(dispatcher.submitCreate(o, i));
        assertEquals("READY", store.state(i));
        assertEquals(List.of(i), sent);
    }

    @Test
    void queueFullAbortsDurablyAndTheCommandIsNeverResent() {
        var o = pendingNew(9001);
        var i = intentFor(o);
        queueAccepts = false;
        assertFalse(dispatcher.submitCreate(o, i));
        assertEquals("ABORTED", store.state(i));
        queueAccepts = true;
        dispatcher.requestResend();
        dispatcher.run();
        assertTrue(sent.isEmpty());
    }

    @Test
    void inProcessAbortWinsOverAStaleOpenPage() {
        var o = pendingNew(9001);
        var i = intentFor(o);
        queueAccepts = false;
        assertFalse(dispatcher.submitCreate(o, i));
        queueAccepts = true;
        store.staleOpenPage = List.of(new Entry(i, 3, "READY"));
        dispatcher.requestResend();
        dispatcher.run();
        assertTrue(sent.isEmpty(), "a page read before the abort must not resurrect the command");
    }

    @Test
    void startupRecoveryReadiesPreparedAndResendsExactBytesOnce() {
        var a = pendingNew(9001);
        var b = pendingNew(9002);
        var legacy = pendingNew(9003); // no durable command: legacy lane, never sent durably
        var ia = prepared(a, true);
        var ib = prepared(b, false);
        dispatcher.recoverAtStartup();
        assertEquals("READY", store.state(ib));
        assertTrue(core.isUnresolved(9001));
        assertTrue(core.isUnresolved(9002));
        assertFalse(core.isUnresolved(legacy.getOmsOrderId()));
        dispatcher.run();
        assertEquals(List.of(ia, ib), sent);
        dispatcher.run();
        assertEquals(2, sent.size(), "no resend without a new session seam");
    }

    @Test
    void reconnectResendsOnlyUnresolvedReadyCommands() {
        var a = pendingNew(9001);
        var b = pendingNew(9002);
        var ia = prepared(a, true);
        var ib = prepared(b, true);
        store.outcomes.put(ib.id(), new DurableCommandStore.Outcome(ib, 55, 0, 0, 0));
        dispatcher.run(); // resolves b
        assertTrue(sent.isEmpty());
        dispatcher.requestResend();
        dispatcher.run();
        assertEquals(List.of(ia), sent);
    }

    @Test
    void resendPagesThroughMoreThanOneBatch() {
        List<DurableOrderIntent> all = new ArrayList<>();
        for (int n = 0; n < DurableCommandDispatcher.BATCH + 3; n++) all.add(prepared(pendingNew(10_000 + n), true));
        dispatcher.requestResend();
        for (int n = 0; n < 4; n++) dispatcher.run();
        assertEquals(all.size(), sent.size());
        assertEquals(new HashSet<>(all), new HashSet<>(sent));
    }

    @Test
    void appliedOutcomeLinksTheOrderAndClearsUncertainty() {
        var o = pendingNew(9001);
        var i = prepared(o, true);
        core.markUnresolved(9001);
        store.outcomes.put(i.id(), new DurableCommandStore.Outcome(i, 77, 0, 0, 0));
        dispatcher.run();
        assertEquals(77, o.getClusterOrderId());
        assertFalse(core.isUnresolved(9001));
        assertEquals("RESOLVED", store.state(i));
        assertEquals(List.of(9001L), persisted);
        dispatcher.run();
        assertEquals(List.of(9001L), persisted, "a resolved outcome is applied once");
    }

    @Test
    void appliedOutcomeWithTerminalStatusKeepsAdmissionClosedUntilEgressTerminal() {
        var o = pendingNew(9001);
        var i = prepared(o, true);
        core.markUnresolved(9001);
        store.outcomes.put(i.id(), new DurableCommandStore.Outcome(i, 77, 2, 0, 0)); // FILLED at application
        dispatcher.run();
        assertEquals(77, o.getClusterOrderId());
        assertTrue(core.isUnresolved(9001), "fills are not inferred from the command outcome");
        assertEquals(OmsOrderStatus.PENDING_NEW, o.getStatus());
    }

    @Test
    void engineRejectionTerminalizesTheOrderOnce() {
        var o = pendingNew(9001);
        var i = prepared(o, true);
        core.markUnresolved(9001);
        store.outcomes.put(i.id(), new DurableCommandStore.Outcome(i, 77, 4, 1, 1));
        dispatcher.run();
        assertEquals(OmsOrderStatus.REJECTED, o.getStatus());
        assertNull(lcm.getOrder(9001));
        assertEquals("RESOLVED", store.state(i));
        assertEquals(0, core.getUnresolvedOrderCount());
    }

    @Test
    void notAppliedResultTerminalizesAnUnsentOrder() {
        for (int result : new int[] {4, 5, 6}) {
            setUp();
            var o = pendingNew(9001);
            var i = prepared(o, true);
            store.outcomes.put(i.id(), new DurableCommandStore.Outcome(i, 0, -1, 0, result));
            dispatcher.run();
            assertEquals(OmsOrderStatus.REJECTED, o.getStatus(), "result " + result);
            assertEquals("RESOLVED", store.state(i));
        }
    }

    @Test
    void outcomeForAnOrderNoLongerActiveOnlyResolvesTheCommand() {
        var o = pendingNew(9001);
        var i = prepared(o, true);
        lcm.onSubmitFailed(9001, "gone"); // e.g. terminal already applied from egress
        store.outcomes.put(i.id(), new DurableCommandStore.Outcome(i, 77, 0, 0, 0));
        dispatcher.run();
        assertEquals("RESOLVED", store.state(i));
        assertTrue(persisted.isEmpty());
    }

    @Test
    void storeFailureDoesNotEscapeTheSchedulerThread() {
        var failing = new DurableCommandDispatcher(new FakeStore() {
            @Override public List<Outcome> projectedOutcomes(int limit) { throw new PersistenceException("down", null); }
        }, core, s -> true);
        assertDoesNotThrow(failing::run);
    }
}
