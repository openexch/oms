// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.match.domain.commands.DurableOrderIntent;
import com.openexchange.oms.cluster.OrderSubmission;
import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.core.OmsCoreEngine;
import com.openexchange.oms.persistence.DurableCommandStore;
import com.openexchange.oms.persistence.PostgresCommandRepository.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Durable ME command lane for plain creates (flag-gated).
 * <p>
 * A command is offered only once its exact payload is READY in Postgres. The matching engine
 * deduplicates by identity, so resending a READY command is always safe; releasing the hold of
 * a command that might still be applied is not. The only local release is an abort of a command
 * this process never offered, committed before the order is rejected. Every other resolution
 * waits for the canonical outcome projected from the Archive.
 * <p>
 * Resends happen at startup and after a session seam, where an offered command may not have
 * reached the log. {@link #run()} is the single scheduler entry point; it never runs on the
 * Aeron polling thread.
 */
public final class DurableCommandDispatcher implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(DurableCommandDispatcher.class);

    /** Enqueue to the cluster client; false means the command was never offered. */
    @FunctionalInterface
    public interface Sender { boolean submit(OrderSubmission submission); }

    static final int BATCH = 256;
    private static final int RESULT_APPLIED = 0, RESULT_REJECTED = 1;
    private static final int STATUS_NEW = 0, STATUS_PARTIALLY_FILLED = 1, STATUS_REJECTED = 4;

    private final DurableCommandStore store;
    private final OmsCoreEngine coreEngine;
    private final Sender sender;

    /** Commands aborted by this process; guards a resend page read before the abort committed. */
    private final Set<UUID> abortedHere = ConcurrentHashMap.newKeySet();
    private volatile boolean resendRequested;
    // Resend cursor, owned by the scheduler thread.
    private boolean resending;
    private long cursorHigh, cursorLow;

    public DurableCommandDispatcher(DurableCommandStore store, OmsCoreEngine coreEngine, Sender sender) {
        this.store = store;
        this.coreEngine = coreEngine;
        this.sender = sender;
    }

    static DurableOrderIntent createIntent(OmsOrder order, long budget,
                                           com.match.infrastructure.generated.OrderType type,
                                           com.match.infrastructure.generated.OrderSide side) {
        // Identity is a pure function of the durable workflow step, so a crash before PREPARED
        // commits re-derives the same command. kind 0 = create.
        long revision = order.getStateRevision();
        if (revision <= 0 || revision >= (1L << 61)) throw new IllegalStateException("Durable workflow revision required");
        return new DurableOrderIntent(order.getOmsOrderId(), revision << 2, order.getUserId(), order.getOmsOrderId(),
                0, order.getPrice(), order.getQuantity(), budget, order.getMarketId(), 0, type.value(), side.value());
    }

    /**
     * Persist, ready and offer a create whose hold is already durable (order PENDING_NEW).
     * On false the command is ABORTED and the caller may terminalize the order.
     */
    public boolean submitCreate(OmsOrder order, DurableOrderIntent intent) {
        synchronized (order) {
            store.prepare(intent, order.getStateRevision());
            store.ready(intent);
            if (sender.submit(OrderSubmission.durable(intent))) return true;
            store.abortUnsent(intent);
            abortedHere.add(intent.id());
            return false;
        }
    }

    /**
     * After the Postgres rebuild and before the cluster session: every open command belongs to an
     * order whose hold is durable, so PREPARED becomes READY. Their orders stay unresolved
     * (admission closed) until the canonical outcome arrives.
     */
    public void recoverAtStartup() {
        long high = Long.MIN_VALUE, low = Long.MIN_VALUE;
        int recovered = 0;
        while (true) {
            var page = store.openCommands(high, low, BATCH);
            for (Entry entry : page) {
                var intent = entry.intent();
                OmsOrder order = coreEngine.getLifecycleManager().getOrder(intent.omsOrderId());
                if (entry.state().equals("PREPARED")) {
                    if (order == null || order.getStatus() != OmsOrderStatus.PENDING_NEW) {
                        throw new IllegalStateException("Prepared ME command without a durable hold: " + intent.omsOrderId());
                    }
                    store.ready(intent);
                }
                if (order != null && order.getClusterOrderId() == 0) coreEngine.markUnresolved(order.getOmsOrderId());
                high = intent.idHigh();
                low = intent.idLow();
                recovered++;
            }
            if (page.size() < BATCH) break;
        }
        log.info("Durable ME commands recovered: {} open command(s) queued for resend", recovered);
        requestResend();
    }

    /** A session seam: an offered command may not have reached the log. */
    public void requestResend() {
        resendRequested = true;
    }

    @Override
    public void run() {
        try {
            applyOutcomes();
            resendStep();
        } catch (RuntimeException e) {
            // The next tick retries; admission stays closed while orders are unresolved.
            log.error("Durable ME command dispatch failed", e);
        }
    }

    private void applyOutcomes() {
        for (DurableCommandStore.Outcome outcome : store.projectedOutcomes(BATCH)) {
            apply(outcome);
            store.resolve(outcome.intent());
        }
    }

    /** Idempotent: a crash before resolve re-applies the same canonical outcome. */
    private void apply(DurableCommandStore.Outcome outcome) {
        long omsOrderId = outcome.intent().omsOrderId();
        var lcm = coreEngine.getLifecycleManager();
        OmsOrder order = lcm.getOrder(omsOrderId);
        if (order == null) return;
        synchronized (order) {
            if (order.isTerminal()) return;
            switch (outcome.result()) {
                case RESULT_APPLIED -> {
                    if (order.getClusterOrderId() == 0 && outcome.orderId() > 0) {
                        lcm.onSentToCluster(omsOrderId, outcome.orderId());
                        coreEngine.persistOrderState(order);
                    }
                    // Only a resting leg resolves the send. A terminal status at application time
                    // still needs its fills/terminal from the execution stream.
                    if (order.getClusterOrderId() == outcome.orderId()
                            && (outcome.status() == STATUS_NEW || outcome.status() == STATUS_PARTIALLY_FILLED)) {
                        coreEngine.clearUnresolved(omsOrderId);
                    }
                }
                case RESULT_REJECTED -> {
                    if (outcome.status() != STATUS_REJECTED) {
                        throw new IllegalStateException("Engine rejection without a rejected status: " + omsOrderId);
                    }
                    OmsOrder rejected = lcm.onClusterOrderStatus(omsOrderId, outcome.orderId(), STATUS_REJECTED,
                            0, order.getFilledQty(), OmsEgressAdapter.mapRejectReason(outcome.reason()));
                    if (rejected != null) coreEngine.persistOrderState(rejected);
                }
                default -> {
                    // Unknown market / leg / owner mismatch: the book was not touched.
                    if (order.getStatus() == OmsOrderStatus.PENDING_NEW && order.getClusterOrderId() == 0) {
                        lcm.onSubmitFailed(omsOrderId, "Matching engine refused command (result " + outcome.result() + ")");
                        coreEngine.persistOrderState(order);
                    }
                }
            }
        }
    }

    private void resendStep() {
        if (!resending) {
            if (!resendRequested) return;
            resendRequested = false;
            resending = true;
            cursorHigh = Long.MIN_VALUE;
            cursorLow = Long.MIN_VALUE;
        }
        var page = store.openCommands(cursorHigh, cursorLow, BATCH);
        int sent = 0;
        for (Entry entry : page) {
            var intent = entry.intent();
            cursorHigh = intent.idHigh();
            cursorLow = intent.idLow();
            // PREPARED belongs to an in-flight submitCreate in this process.
            if (!entry.state().equals("READY")) continue;
            if (resend(intent)) sent++;
        }
        if (page.size() < BATCH) resending = false;
        if (sent > 0) log.info("Durable ME commands resent: {}", sent);
    }

    private boolean resend(DurableOrderIntent intent) {
        OmsOrder order = coreEngine.getLifecycleManager().getOrder(intent.omsOrderId());
        if (order == null) return !abortedHere.contains(intent.id()) && offer(intent);
        synchronized (order) {
            return !abortedHere.contains(intent.id()) && offer(intent);
        }
    }

    private boolean offer(DurableOrderIntent intent) {
        if (sender.submit(OrderSubmission.durable(intent))) return true;
        // Full queue: nothing was offered. Retry on the next session seam or tick.
        requestResend();
        return false;
    }
}
