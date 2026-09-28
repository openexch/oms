// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.openexchange.oms.assets.AeronAssetsBalanceStore;
import com.openexchange.oms.assets.HoldSnapshotConsumer;
import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.domain.SnowflakeIdGenerator;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.core.OrderLifecycleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Read-only hold discrepancy detector. Neither a missing intent nor a missing ME
 * open-order entry proves that a hold is releasable. Age and repeated snapshots
 * do not strengthen that proof. Financial repair belongs to the durable command
 * owner / settlement feed, which can establish the actual submission outcome.
 * Snapshot ingestion stays on the poll thread; PG lookups run on the scheduler.
 */
public final class AssetsHoldReconciler implements HoldSnapshotConsumer {

    private static final Logger log = LoggerFactory.getLogger(AssetsHoldReconciler.class);

    /** Age gate: an order younger than this is never released (its CreateOrder may still be in flight). */
    static final long MIN_AGE_MS = 60_000;

    /** Sweep cadence. */
    private static final long SWEEP_PERIOD_MS = 60_000;

    /** Fallback arm delay: if the ME open-orders reconcile never completes (e.g. ME down), begin
     *  sweeping anyway after this long so the backstop is never disabled forever. */
    private static final long FALLBACK_ARM_MS = 120_000;

    /** Clock skew tolerance when sanity-checking a decoded Snowflake timestamp against "now". */
    private static final long CLOCK_SKEW_MS = 60_000;

    /** The lookup seam for the PG {@code orders} table (typically {@code PostgresOrderRepository::findById}). */
    @FunctionalInterface
    public interface OrderLookup {
        /** @return the persisted order, or {@code null} if there is no row (or PG is unavailable). */
        OmsOrder findById(long omsOrderId);
    }

    /** One outstanding AE hold. {@code orderId} is the OMS {@code omsOrderId}. Package-private: the
     *  predicate unit tests drive {@link #classify} and {@link #processSnapshot} with these. */
    record HoldEntry(long orderId, long userId, int assetId, long remaining) {
    }

    enum Kind {
        /** A live (non-terminal) OMS order holds these funds legitimately — not an orphan. */
        ACTIVE_LEGIT,
        /** Cannot be proven never-submitted (reached the cluster / ambiguous): surfaced for a human. */
        SURFACE,
        /** An orphan that is not yet release-eligible for a benign reason (inside the age gate). */
        PENDING
    }

    record Decision(Kind kind, String reason) {
    }

    private final AeronAssetsBalanceStore store;
    private final OrderLifecycleManager lifecycle;
    private final OrderLookup pgLookup;
    private final Clock clock;
    private final java.util.function.LongPredicate clusterOpenOmsOrderIds;

    private final AtomicLong correlationIds =
            new AtomicLong(ThreadLocalRandom.current().nextLong(1, 1L << 40));
    private final AtomicBoolean armed = new AtomicBoolean(false);

    private volatile ScheduledExecutorService scheduler;

    /** Accumulator for the in-flight sweep's snapshot; null between sweeps. Written by the scheduler
     *  thread (sweep) and the poll thread (entries/end), handed off via {@link #scheduler}. */
    private volatile List<HoldEntry> currentSnapshot;
    private volatile long expectedCorrelationId;

    /** Orphans that were release-eligible in the PREVIOUS sweep. Touched only on the scheduler thread
     *  (in {@link #processSnapshot}), so a plain field reassignment is safe. */

    // ---- metrics (read by the /metrics scrape thread) ----
    private final AtomicLong sweepsTotal = new AtomicLong();
    private final AtomicLong processedSnapshots = new AtomicLong();

    public long getProcessedSnapshots() { return processedSnapshots.get(); }

    private final AtomicLong orphanReleasesTotal = new AtomicLong();
    private volatile long unresolvedOrphansLastSweep;

    public AssetsHoldReconciler(final AeronAssetsBalanceStore store,
                                final OrderLifecycleManager lifecycle,
                                final OrderLookup pgLookup,
                                final Clock clock) {
        this(store, lifecycle, pgLookup, clock, omsOrderId -> false);
    }

    public AssetsHoldReconciler(final AeronAssetsBalanceStore store,
                                final OrderLifecycleManager lifecycle,
                                final OrderLookup pgLookup,
                                final Clock clock,
                                final java.util.function.LongPredicate clusterOpenOmsOrderIds) {
        this.store = store;
        this.lifecycle = lifecycle;
        this.pgLookup = pgLookup;
        this.clock = clock;
        this.clusterOpenOmsOrderIds = clusterOpenOmsOrderIds;
    }

    /**
     * Attach the hold-snapshot forwarding seam and stand up the scheduler, but do NOT begin sweeping
     * yet. Call {@link #onStartupReconcileComplete()} once the startup ME open-orders reconcile has
     * finished; a fallback arms sweeps anyway after {@value #FALLBACK_ARM_MS} ms so a never-connecting
     * ME cannot silently disable the backstop.
     */
    public void start() {
        store.setHoldSnapshotConsumer(this);
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "oms-assets-reconciler");
            t.setDaemon(true);
            return t;
        });
        scheduler.schedule(() -> arm("startup-timeout fallback"), FALLBACK_ARM_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Signal that the startup ME open-orders reconcile has completed (wired to the first post-reconcile
     * hook in {@code OmsApplication}). Fires the initial sweep and starts the {@value #SWEEP_PERIOD_MS}
     * ms cadence. Idempotent: only the first call arms; later reconciles are no-ops here.
     */
    public void onStartupReconcileComplete() {
        arm("me-open-orders reconcile complete");
    }

    private void arm(final String reason) {
        final ScheduledExecutorService s = scheduler;
        if (s == null || s.isShutdown()) {
            return;
        }
        if (!armed.compareAndSet(false, true)) {
            return; // already armed
        }
        log.info("Assets orphan-hold reconciler armed ({}); initial sweep now, then every {}ms",
                reason, SWEEP_PERIOD_MS);
        s.scheduleAtFixedRate(this::sweepSafely, 0, SWEEP_PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    private void sweepSafely() {
        try {
            sweep();
        } catch (Exception e) {
            log.error("Assets orphan-hold sweep failed", e);
        }
    }

    /**
     * One sweep: issue a hold-snapshot request against the AE. The predicate runs later, when the AE
     * answers ({@link #onHoldSnapshotEnd}). Skips when the AE projection is not ready (a stale or
     * disconnected AE view must never drive releases).
     */
    void sweep() {
        sweepsTotal.incrementAndGet();
        if (!store.isProjectionReady()) {
            log.debug("Skipping orphan-hold sweep: AE projection not ready");
            return;
        }
        if (currentSnapshot != null) {
            log.debug("Previous hold snapshot did not complete before this sweep; abandoning it");
        }
        final long corr = correlationIds.incrementAndGet();
        final List<HoldEntry> acc = new ArrayList<>();
        this.currentSnapshot = acc;          // volatile publish before the request
        this.expectedCorrelationId = corr;
        if (!store.requestHoldSnapshot(corr)) {
            log.warn("Hold-snapshot request back-pressured; will retry next sweep");
            this.currentSnapshot = null;
        }
    }

    // ==================== HoldSnapshotConsumer (oms-assets-poll thread) ====================

    @Override
    public void onHoldSnapshotEntry(final long orderId, final long userId, final int assetId,
                                    final long remaining) {
        // Zero-remaining records are settle-consumed TOMBSTONES the AE never removes (assets#5):
        // they hold no money, cannot be released, and at storm scale (150k+) they drown the
        // orphan metric and the sweep in noise. Money-bearing holds only.
        if (remaining <= 0) {
            return;
        }
        final List<HoldEntry> acc = currentSnapshot;
        if (acc != null) {
            acc.add(new HoldEntry(orderId, userId, assetId, remaining));
        }
    }

    @Override
    public void onHoldSnapshotEnd(final long correlationId, final int entryCount) {
        if (correlationId != expectedCorrelationId) {
            log.debug("Ignoring stale hold-snapshot end: corr={} expected={}",
                    correlationId, expectedCorrelationId);
            return;
        }
        final List<HoldEntry> acc = currentSnapshot;
        this.currentSnapshot = null;
        if (acc == null) {
            return;
        }
        // Hand off to the scheduler thread: classification does JDBC (PG lookups) and iteration,
        // which must never run on the single Aeron poll thread.
        final ScheduledExecutorService s = scheduler;
        if (s != null && !s.isShutdown()) {
            s.execute(() -> processSnapshot(acc));
        }
    }

    // ==================== predicate + release (scheduler thread) ====================

    /** Per-sweep exemplar budget for the aggregated SURFACE/PENDING/candidate classes. */
    private static final int LOG_EXEMPLARS_PER_SWEEP = 3;

    void processSnapshot(final List<HoldEntry> entries) {
        final long now = clock.millis();
        long unresolved = 0;
        long activeLegit = 0;
        // The steady-state classes (SURFACE/PENDING/first-observation) are AGGREGATED: with a
        // large orphan backlog the old per-hold line logged every orphan every sweep — 13M+
        // lines/hour, ~1MB/s of disk (the 2026-07-11 storm). A few exemplars + per-reason counts
        // carry the same diagnostic signal. RELEASED and back-pressure stay per-hold (rare, and
        // each one is a money-state action that must be individually auditable).
        long surfaced = 0;
        long pending = 0;
        final Map<String, Long> unresolvedByReason = new HashMap<>();

        for (HoldEntry h : entries) {
            final Decision d = classify(h, now);
            switch (d.kind()) {
                case ACTIVE_LEGIT -> {
                    activeLegit++;
                    if (log.isDebugEnabled()) {
                        log.debug("Hold backed by a live order (skip): orderId={} user={} asset={} remaining={}",
                                h.orderId(), h.userId(), h.assetId(), h.remaining());
                    }
                }
                case SURFACE -> {
                    unresolved++;
                    surfaced++;
                    unresolvedByReason.merge(d.reason(), 1L, Long::sum);
                    if (surfaced <= LOG_EXEMPLARS_PER_SWEEP) {
                        log.warn("Orphan hold UNRESOLVED (not provably releasable — surfaced for a human): "
                                        + "orderId={} user={} asset={} remaining={} reason={}",
                                h.orderId(), h.userId(), h.assetId(), h.remaining(), d.reason());
                    }
                }
                case PENDING -> {
                    unresolved++;
                    pending++;
                    if (pending <= LOG_EXEMPLARS_PER_SWEEP) {
                        log.info("Orphan hold pending (not yet release-eligible): orderId={} user={} asset={} "
                                        + "remaining={} reason={}",
                                h.orderId(), h.userId(), h.assetId(), h.remaining(), d.reason());
                    }
                }
            }
        }

        this.unresolvedOrphansLastSweep = unresolved;
        processedSnapshots.incrementAndGet();
        log.info("Hold discrepancy scan: {} holds, {} live, {} unresolved ({} surfaced, {} pending); reasons={}",
                entries.size(), activeLegit, unresolved, surfaced, pending, unresolvedByReason);
    }

    /**
     * Classify one hold against the release contract. Pure w.r.t. lifecycle/PG state — the unit tests
     * drive every branch through here.
     */
    Decision classify(final HoldEntry h, final long now) {
        // Condition 1: an active (non-terminal) OMS order for this omsOrderId => the hold is legit.
        final OmsOrder inLifecycle = lifecycle.getOrder(h.orderId());
        if (inLifecycle != null && !inLifecycle.getStatus().isTerminal()) {
            return new Decision(Kind.ACTIVE_LEGIT, "live order in lifecycle");
        }

        // TIGHTENING (closes the crash-after-submit-before-first-persist residual): the latest ME
        // open-orders snapshot is the authority on "reached the cluster". A hold whose omsOrderId
        // is OPEN ON THE CLUSTER with no active OMS record is a crash-lost RESTING order: its fills
        // are still coming via the settlement feed, so it is SURFACED for repair, never released.
        // (Already-terminal-on-ME orders were handled by the lossless journal feed; genuinely
        // never-submitted ids can never appear in the snapshot.)
        if (clusterOpenOmsOrderIds.test(h.orderId())) {
            return new Decision(Kind.SURFACE,
                    "omsOrderId is OPEN on the cluster per the latest ME snapshot (crash-lost "
                            + "resting order — needs repair, not release)");
        }

        // No active order. Gather the fullest record we have: a terminal straggler still in the map,
        // else the persisted row (if any).
        OmsOrder known = inLifecycle; // may be a terminal order lingering in the map (rare race)
        if (known == null && pgLookup != null) {
            known = pgLookup.findById(h.orderId());
        }
        final long cid = known != null ? known.getClusterOrderId() : 0L;
        final OmsOrderStatus st = known != null ? known.getStatus() : null;

        // Reached the cluster (a clusterOrderId was assigned) => cannot prove never-submitted. Its
        // release is the settlement feed's TerminalRelease (or already happened); surface any lingering
        // hold for a human. This is contract's "terminal in OMS but DID reach the cluster => surfaced".
        if (known != null && cid != 0L) {
            final boolean terminal = st != null && st.isTerminal();
            return new Decision(Kind.SURFACE, (terminal ? "terminal" : "non-terminal")
                    + " order reached the cluster (clusterOrderId=" + cid + ")");
        }

        if (known == null) {
            // Submission may have happened before the missing record was persisted.
            if (!snowflakeAgeOk(h.orderId(), now)) {
                return new Decision(Kind.PENDING, "unknown order younger than the age gate "
                        + "(CreateOrder may still be in flight)");
            }
            return new Decision(Kind.SURFACE, "missing intent: submission outcome is unknown");
        }

        // known && clusterOrderId == 0.
        if (st != null && st.isTerminal()) {
            // A missing cluster id can also be a lost acknowledgement.
            if (!orderAgeOk(known, now)) {
                return new Decision(Kind.PENDING, "terminal pre-cluster order younger than the age gate");
            }
            return new Decision(Kind.SURFACE, "terminal with no cluster id: missing ack is not abort proof");
        }
        if (isPreClusterClass(st)) {
            // Persisted stage can lag a completed external action.
            if (!orderAgeOk(known, now)) {
                return new Decision(Kind.PENDING, "pre-cluster order younger than the age gate "
                        + "(may still be in the submit path)");
            }
            return new Decision(Kind.SURFACE, "unresolved intent (" + st + "): age is not abort proof");
        }

        // known, clusterOrderId == 0, but a post-cluster status (NEW/PARTIALLY_FILLED) or a synthetic
        // parent (PENDING_TRIGGER): an ack-lost zombie may actually be resting on the cluster under
        // this omsOrderId, or the parent legitimately holds for its children. Cannot prove
        // never-submitted => surface.
        return new Decision(Kind.SURFACE, "status=" + st + " with no clusterOrderId "
                + "(ack-lost zombie or synthetic parent; may be live on the cluster)");
    }

    private static boolean isPreClusterClass(final OmsOrderStatus st) {
        return st == OmsOrderStatus.PENDING_RISK
                || st == OmsOrderStatus.PENDING_HOLD
                || st == OmsOrderStatus.PENDING_NEW;
    }

    /** Age gate for a known order: its persisted/lifecycle createdAt, falling back to the id's own time. */
    private boolean orderAgeOk(final OmsOrder order, final long now) {
        final long createdAt = order.getCreatedAtMs();
        if (createdAt <= 0) {
            return snowflakeAgeOk(order.getOmsOrderId(), now);
        }
        return now - createdAt >= MIN_AGE_MS;
    }

    /**
     * Age gate for an order the OMS has no record of: decode the omsOrderId's Snowflake timestamp.
     * Fails closed (returns false => never released) if the id does not decode to a plausible time,
     * so a non-Snowflake or clock-skewed id can never be released on age grounds alone.
     */
    private boolean snowflakeAgeOk(final long orderId, final long now) {
        final long ts = SnowflakeIdGenerator.timestampMillis(orderId);
        if (ts > now + CLOCK_SKEW_MS) {
            return false; // decodes to the future: not a trustworthy age
        }
        return now - ts >= MIN_AGE_MS;
    }

    /** Detach the forwarding seam and stop the scheduler. */
    public void stop() {
        final ScheduledExecutorService s = scheduler;
        if (s != null) {
            s.shutdownNow();
        }
        store.setHoldSnapshotConsumer(null);
    }

    // ==================== metrics accessors ====================

    /** oms_assets_orphan_releases_total */
    public long getOrphanReleasesTotal() {
        return orphanReleasesTotal.get();
    }

    /** oms_assets_unresolved_orphans (last sweep) */
    public long getUnresolvedOrphansLastSweep() {
        return unresolvedOrphansLastSweep;
    }

    /** oms_assets_reconciler_sweeps_total */
    public long getSweepsTotal() {
        return sweepsTotal.get();
    }
}
