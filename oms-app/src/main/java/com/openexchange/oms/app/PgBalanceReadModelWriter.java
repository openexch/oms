// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.openexchange.oms.assets.BalanceChangeConsumer;
import com.openexchange.oms.persistence.BalanceReadModelRow;
import com.openexchange.oms.persistence.BalanceReadModelStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The CQRS PG balance read-model writer: a durable, queryable mirror of AE balances for
 * ops/analytics. Fed by the {@link com.openexchange.oms.assets.AeronAssetsBalanceStore} change-tap
 * on the single {@code oms-assets-poll} thread; drains to Postgres on its OWN single daemon thread.
 *
 * <h2>Why a keyed dirty-map, not a queue</h2>
 * <p>The tap delivers ABSOLUTE {@code (available, locked)} per {@code (user, asset)}. A bounded MPSC
 * queue is the wrong structure here: on overflow, drop-oldest loses a mutation and drop-newest loses
 * the <i>current truth</i> — both leave the mirror wrong. The right structure is last-write-wins per
 * key: a {@link ConcurrentHashMap} keyed by {@code (user, asset)} whose value is the latest absolutes
 * (presence in the map IS the dirty flag). The poll thread just publishes the newest value (a later
 * publish overwrites an unflushed earlier one — no loss, no unbounded growth: memory is bounded by
 * the active key-space, not by update rate). The writer drains the whole map every
 * {@value #DEFAULT_FLUSH_INTERVAL_MS}ms with one batched upsert.</p>
 *
 * <h2>Failure posture</h2>
 * <p>PG down is non-fatal and never blocks the poll thread: the map keeps accumulating (coalescing),
 * a failed flush leaves everything dirty, and the next tick retries. Consecutive failures are counted
 * and logged at most once per minute (not once per tick) so a PG outage does not flood the log.</p>
 *
 * <h2>Consistency</h2>
 * <p>Eventually consistent by design — a key is at most one flush interval behind the projection, and
 * a slot the writer drained but PG applied a beat before a fresh publish is simply re-marked dirty
 * and rewritten next tick (last-write-wins is preserved via a value-checked remove). This mirror is
 * <b>never</b> money-authoritative and is <b>never</b> read by the money path.</p>
 */
public final class PgBalanceReadModelWriter implements BalanceChangeConsumer {

    private static final Logger log = LoggerFactory.getLogger(PgBalanceReadModelWriter.class);

    /** Default drain cadence. */
    static final long DEFAULT_FLUSH_INTERVAL_MS = 500;

    /** Throttle window for the "PG failing" WARN so an outage logs once/min, not once/tick. */
    private static final long WARN_INTERVAL_MS = 60_000;

    /** Dirty-map key. A record gives value-based equals/hashCode; userIds are full 63-bit
     *  Snowflakes so packing into a single long is unsafe — a small key allocation per change is the
     *  price of correctness on a read-model tap. */
    record Key(long userId, int assetId) {
    }

    private final BalanceReadModelStore store;
    private final long flushIntervalMs;

    /** (user, asset) -> latest absolute {@code {available, locked}}. Presence == dirty. */
    private final ConcurrentHashMap<Key, long[]> dirty = new ConcurrentHashMap<>();

    private volatile ScheduledExecutorService scheduler;

    // ---- metrics (monotonic counters read by the /metrics scrape thread) ----
    private final AtomicLong rowsWritten = new AtomicLong();
    private final AtomicLong flushFailures = new AtomicLong();

    // ---- flush-thread-local bookkeeping (only ever touched by the single active flusher) ----
    private volatile long consecutiveFailures;
    private volatile long lastWarnAtMs;

    public PgBalanceReadModelWriter(final BalanceReadModelStore store) {
        this(store, DEFAULT_FLUSH_INTERVAL_MS);
    }

    public PgBalanceReadModelWriter(final BalanceReadModelStore store, final long flushIntervalMs) {
        this.store = store;
        this.flushIntervalMs = flushIntervalMs;
    }

    // ==================== BalanceChangeConsumer (oms-assets-poll thread) ====================

    @Override
    public void onBalanceChange(final long userId, final int assetId, final long available,
                                final long locked) {
        // Last-write-wins: overwrite any unflushed value for this key. Minimal work on the poll thread.
        dirty.put(new Key(userId, assetId), new long[] {available, locked});
    }

    // ==================== lifecycle ====================

    /** Stand up the single daemon flush thread and begin draining every {@code flushIntervalMs}. */
    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "oms-balance-readmodel");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::flushSafely, flushIntervalMs, flushIntervalMs,
                TimeUnit.MILLISECONDS);
        log.info("PG balance read-model writer started (flush every {}ms)", flushIntervalMs);
    }

    /** Stop the flush thread and do one final drain so nothing dirty is lost on graceful shutdown. */
    public void stop() {
        final ScheduledExecutorService s = scheduler;
        if (s != null) {
            s.shutdown();
            try {
                if (!s.awaitTermination(2, TimeUnit.SECONDS)) {
                    s.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                s.shutdownNow();
            }
        }
        // The scheduler thread is now quiescent, so this final flush runs with no concurrent flusher.
        final int wrote = flushOnce();
        log.info("PG balance read-model writer stopped (final flush wrote {} rows, {} still dirty)",
                wrote, dirty.size());
    }

    private void flushSafely() {
        try {
            flushOnce();
        } catch (Throwable t) {
            // flushOnce already handles PG failures; this catches anything truly unexpected so the
            // scheduled task is never silently cancelled.
            log.error("PG balance read-model flush hit an unexpected error", t);
        }
    }

    /**
     * Drain the whole dirty map into one batched upsert. On success, clear exactly the entries we
     * wrote (value-checked, so a value the poll thread refreshed mid-flush survives to next tick). On
     * failure, everything stays dirty and is retried next tick.
     *
     * @return number of rows written (0 if nothing was dirty or the flush failed)
     */
    int flushOnce() {
        if (dirty.isEmpty()) {
            return 0;
        }
        // Snapshot the current dirty set WITHOUT removing — so a PG failure keeps every row dirty.
        final List<BalanceReadModelRow> batch = new ArrayList<>(dirty.size());
        final List<Map.Entry<Key, long[]>> taken = new ArrayList<>(dirty.size());
        for (Map.Entry<Key, long[]> e : dirty.entrySet()) {
            final Key k = e.getKey();
            final long[] v = e.getValue();
            batch.add(new BalanceReadModelRow(k.userId(), k.assetId(), v[0], v[1]));
            taken.add(Map.entry(k, v));
        }

        try {
            store.upsert(batch);
        } catch (Exception ex) {
            flushFailures.incrementAndGet();
            consecutiveFailures = consecutiveFailures + 1;
            maybeWarn(consecutiveFailures, batch.size(), ex);
            return 0; // keep the dirt; retry next tick
        }

        // Success: remove only the exact array instances we wrote. A newer publish installed a new
        // array (different identity), so remove(key, oldArray) leaves it dirty for the next tick.
        for (Map.Entry<Key, long[]> e : taken) {
            dirty.remove(e.getKey(), e.getValue());
        }
        rowsWritten.addAndGet(batch.size());
        if (consecutiveFailures != 0) {
            log.info("PG balance read-model flush recovered after {} consecutive failures",
                    consecutiveFailures);
            consecutiveFailures = 0;
        }
        return batch.size();
    }

    private void maybeWarn(final long consecutive, final int pending, final Exception ex) {
        final long now = System.currentTimeMillis();
        if (now - lastWarnAtMs >= WARN_INTERVAL_MS) {
            lastWarnAtMs = now;
            log.warn("PG balance read-model flush failing ({} consecutive), {} rows pending — keeping "
                    + "them dirty and retrying next tick: {}", consecutive, pending, ex.toString());
        }
    }

    // ==================== metrics accessors ====================

    /** oms_balance_readmodel_rows_written_total */
    public long getRowsWritten() {
        return rowsWritten.get();
    }

    /** oms_balance_readmodel_flush_failures_total */
    public long getFlushFailures() {
        return flushFailures.get();
    }

    /** oms_balance_readmodel_dirty (gauge) */
    public long getDirtyCount() {
        return dirty.size();
    }
}
