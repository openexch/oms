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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Q3 orphan-hold reconciler for the Assets Engine (AE) money store.
 *
 * <h2>The orphan</h2>
 * <p>When the OMS crashes (or the AE session dies with the outcome unknown) between the AE
 * {@code HoldAck} and the cluster {@code CreateOrder} submit, a hold lives on in the AE with no live
 * order anywhere. The AE cannot expire it — it has NO hold TTL by design (a timer expiry would race
 * an in-flight fill and release funds a settle is about to draw). So the OMS must find such orphans
 * and release them. The {@link AeronAssetsBalanceStore} timeout compensator is the first line of
 * defence; this reconciler is the backstop for the cases the compensator could not enqueue or that
 * outlived a whole process restart.</p>
 *
 * <h2>The hazard (why the release predicate is deliberately tight)</h2>
 * <p>Releasing a hold whose order might still receive fills makes a later ME-journal settle draw
 * from a hold that is already gone: a {@code SettleFault}. That is survivable since D5, but it is a
 * wrong money posture, so the predicate releases <b>only a provably-never-submitted hold</b>. Every
 * hold that cannot be proven never-submitted is surfaced (WARN + a gauge), never swept — a human
 * decides.</p>
 *
 * <h2>The contract — release IFF ALL of</h2>
 * <ol>
 *   <li>The OMS has NO active (non-terminal) order with that omsOrderId in the
 *       {@link OrderLifecycleManager}; AND</li>
 *   <li>EITHER
 *     <ul>
 *       <li>(a) the OMS has NO record of the order at all (not in the lifecycle map, not in the PG
 *           {@code orders} table) — the crash-before-persist case; OR</li>
 *       <li>(b) the order IS known and provably never reached the cluster: {@code clusterOrderId==0}
 *           AND a pre-cluster status ({@code PENDING_RISK}/{@code PENDING_HOLD}/{@code PENDING_NEW})
 *           AND it is past the age gate (so it is not still inside the submit path); OR</li>
 *       <li>(c) the order is known and TERMINAL in the OMS with zero fills possible: terminal AND
 *           {@code clusterOrderId} was never assigned; AND</li>
 *     </ul></li>
 *   <li>Age gate: the order (or, when unknown, the omsOrderId's own Snowflake timestamp) is at least
 *       {@value #MIN_AGE_MS} ms old; AND</li>
 *   <li>The same orphan was observed release-eligible in TWO consecutive sweeps (the candidate set is
 *       kept between sweeps; the release fires on the second observation).</li>
 * </ol>
 *
 * <h2>Persist-vs-submit ordering (verified in code — bears on case 2a)</h2>
 * <p>{@code OmsOrderServiceImpl.createOrder} does the AE hold (step 4) and then
 * {@code clusterClient.submitOrder} (step 6); it never writes the order to Postgres inline.
 * Persistence is <b>egress-driven and asynchronous</b>: {@code persistOrderUpdate} is only ever
 * called from {@code OmsCoreEngine} on cluster egress (the first {@code OrderStatus}=NEW ack, a
 * {@code TradeExecution} fill, GTD expiry, or membership repair). The pre-cluster lifecycle
 * transitions (PENDING_RISK -&gt; PENDING_HOLD -&gt; PENDING_NEW) route only through the state
 * listener (WS/gRPC push + hold release), which does not persist. Consequences:</p>
 * <ul>
 *   <li>An order's FIRST Postgres row appears strictly AFTER the submit, when the cluster's NEW
 *       egress is processed. So "no PG record" (case 2a) spans BOTH the intended crash-before-submit
 *       window AND a residual crash-after-submit-before-first-egress-persist window in which the
 *       order MAY have reached the cluster.</li>
 *   <li>A genuinely pre-cluster order (PENDING_RISK/HOLD/NEW with {@code clusterOrderId==0}) is,
 *       in the current codebase, essentially never in Postgres — so cases 2b and 2c via PG are
 *       largely defensive/forward-looking. They DO fire for a terminal straggler still in the
 *       lifecycle map, and for any future change that persists pre-cluster rows.</li>
 * </ul>
 * <p>The residual 2a window is mitigated (not eliminated) by: the 60s age gate, the two-sweep
 * confirmation, and the initial sweep running only AFTER the startup ME open-orders reconcile has
 * had its chance to re-link ack-lost orders. It is surfaced here and left for the design owner to
 * decide whether to add a cluster-open-orders cross-check; the reconciler does not widen the
 * predicate past the contract on its own.</p>
 *
 * <h2>omsOrderId generation (verified — enables the unknown-order age gate)</h2>
 * <p>omsOrderIds come from {@link SnowflakeIdGenerator}: a 41-bit millisecond timestamp in the high
 * bits. {@link SnowflakeIdGenerator#timestampMillis(long)} recovers the creation time, so a hold
 * whose order the OMS has no record of can still be age-gated by its own id.</p>
 *
 * <h2>Threading</h2>
 * <p>{@link #sweep()} runs on this reconciler's single-threaded scheduler and only ISSUES a hold
 * snapshot request (non-blocking). The AE answers on the {@code oms-assets-poll} thread, which feeds
 * {@link #onHoldSnapshotEntry} (cheap accumulation only) and {@link #onHoldSnapshotEnd}. To keep the
 * poll thread free of JDBC and iteration, {@code onHoldSnapshotEnd} hands the accumulated entries
 * back to the scheduler thread, where classification (including PG lookups), the two-sweep
 * bookkeeping, and the {@code releaseAll} calls all run. Releases go through
 * {@link AeronAssetsBalanceStore#releaseAll} (idempotent; double-release-safe with the settlement
 * feed).</p>
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
        /** Passes contract conditions 1-3: subject to the two-sweep gate before release. */
        ELIGIBLE,
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
    private Set<Long> priorCandidates = new HashSet<>();

    // ---- metrics (read by the /metrics scrape thread) ----
    private final AtomicLong sweepsTotal = new AtomicLong();
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
        final Set<Long> newCandidates = new HashSet<>();
        long unresolved = 0;
        long released = 0;
        long activeLegit = 0;
        // The steady-state classes (SURFACE/PENDING/first-observation) are AGGREGATED: with a
        // large orphan backlog the old per-hold line logged every orphan every sweep — 13M+
        // lines/hour, ~1MB/s of disk (the 2026-07-11 storm). A few exemplars + per-reason counts
        // carry the same diagnostic signal. RELEASED and back-pressure stay per-hold (rare, and
        // each one is a money-state action that must be individually auditable).
        long surfaced = 0;
        long pending = 0;
        long firstObservations = 0;
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
                case ELIGIBLE -> {
                    if (priorCandidates.contains(h.orderId())) {
                        // Second consecutive eligible observation -> release.
                        final boolean enqueued = store.releaseAll(h.userId(), h.assetId(), h.orderId());
                        if (enqueued) {
                            orphanReleasesTotal.incrementAndGet();
                            released++;
                            log.warn("Orphan hold RELEASED (provably never submitted): orderId={} user={} "
                                            + "asset={} remaining={} reason={}",
                                    h.orderId(), h.userId(), h.assetId(), h.remaining(), d.reason());
                        } else {
                            // Release could not be enqueued (back-pressure): keep it confirmed and retry.
                            newCandidates.add(h.orderId());
                            unresolved++;
                            log.warn("Orphan hold release back-pressured; will retry next sweep: orderId={} reason={}",
                                    h.orderId(), d.reason());
                        }
                    } else {
                        // First eligible observation -> hold as a candidate for the next sweep.
                        newCandidates.add(h.orderId());
                        unresolved++;
                        firstObservations++;
                        if (firstObservations <= LOG_EXEMPLARS_PER_SWEEP) {
                            log.info("Orphan hold candidate (1st confirmed observation, releases next sweep if "
                                            + "still present): orderId={} user={} asset={} remaining={} reason={}",
                                    h.orderId(), h.userId(), h.assetId(), h.remaining(), d.reason());
                        }
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

        this.priorCandidates = newCandidates;
        this.unresolvedOrphansLastSweep = unresolved;
        log.info("Orphan-hold sweep processed {} holds: {} live, {} released, {} unresolved "
                        + "({} surfaced, {} pending, {} first-observations; exemplars capped at {}/class), "
                        + "{} candidates carried; surfaced by reason: {}",
                entries.size(), activeLegit, released, unresolved,
                surfaced, pending, firstObservations, LOG_EXEMPLARS_PER_SWEEP,
                newCandidates.size(), unresolvedByReason);
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
            // Contract 2(a): NO record at all. See class doc on the persist-after-submit residual.
            if (!snowflakeAgeOk(h.orderId(), now)) {
                return new Decision(Kind.PENDING, "unknown order younger than the age gate "
                        + "(CreateOrder may still be in flight)");
            }
            return new Decision(Kind.ELIGIBLE, "no OMS record (2a: crash-before-persist)");
        }

        // known && clusterOrderId == 0.
        if (st != null && st.isTerminal()) {
            // Contract 2(c): terminal AND cluster order never assigned => zero fills possible.
            if (!orderAgeOk(known, now)) {
                return new Decision(Kind.PENDING, "terminal pre-cluster order younger than the age gate");
            }
            return new Decision(Kind.ELIGIBLE, "terminal, cluster order never assigned (2c)");
        }
        if (isPreClusterClass(st)) {
            // Contract 2(b): known, provably pre-cluster, cluster order never assigned. The age gate
            // stands in for "not currently in the submit path".
            if (!orderAgeOk(known, now)) {
                return new Decision(Kind.PENDING, "pre-cluster order younger than the age gate "
                        + "(may still be in the submit path)");
            }
            return new Decision(Kind.ELIGIBLE, "known pre-cluster (" + st + "), cluster order never assigned (2b)");
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
