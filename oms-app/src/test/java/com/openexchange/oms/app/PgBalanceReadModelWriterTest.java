// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.openexchange.oms.persistence.BalanceReadModelRow;
import com.openexchange.oms.persistence.BalanceReadModelStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the CQRS balance read-model writer's structure: the keyed dirty-map coalesces many updates to
 * one key into a single row per flush (last-write-wins), a failed flush keeps everything dirty for
 * the next tick (no loss), a value refreshed mid-flush survives, and {@code stop()} does a final
 * drain. No PG: the repo has no PG-integration harness (no testcontainers/embedded-pg), so this
 * exercises the writer against a fake {@link BalanceReadModelStore} upsert seam.
 */
class PgBalanceReadModelWriterTest {

    /** Fake upsert sink: records each batch, can be armed to fail, and can run a hook mid-upsert. */
    private static final class FakeStore implements BalanceReadModelStore {
        final List<List<BalanceReadModelRow>> batches = new CopyOnWriteArrayList<>();
        volatile boolean fail = false;
        volatile Runnable onUpsert; // runs INSIDE upsert, before recording — to simulate a concurrent publish

        @Override
        public void upsert(List<BalanceReadModelRow> rows) throws Exception {
            if (onUpsert != null) {
                onUpsert.run();
            }
            if (fail) {
                throw new IllegalStateException("simulated PG down");
            }
            batches.add(new ArrayList<>(rows)); // defensive copy (the writer reuses no list, but be safe)
        }

        /** The single row for a key in the most recent batch, or null. */
        BalanceReadModelRow latestFor(long userId, int assetId) {
            for (int i = batches.size() - 1; i >= 0; i--) {
                for (BalanceReadModelRow r : batches.get(i)) {
                    if (r.userId() == userId && r.assetId() == assetId) {
                        return r;
                    }
                }
            }
            return null;
        }

        int totalRows() {
            int n = 0;
            for (List<BalanceReadModelRow> b : batches) {
                n += b.size();
            }
            return n;
        }
    }

    private FakeStore store;
    private PgBalanceReadModelWriter writer;

    @BeforeEach
    void setUp() {
        store = new FakeStore();
        writer = new PgBalanceReadModelWriter(store); // flush loop not started; drive flushOnce() directly
    }

    @Test
    void manyUpdatesToOneKeyCoalesceToASingleLatestRowPerFlush() {
        for (long v = 1; v <= 100; v++) {
            writer.onBalanceChange(7L, 0, 1_000L - v, v); // 100 updates, same key
        }
        assertEquals(1, writer.getDirtyCount(), "coalesced to one dirty key");

        int wrote = writer.flushOnce();
        assertEquals(1, wrote, "one row for the one key");
        assertEquals(1, store.batches.size());
        assertEquals(1, store.batches.get(0).size());
        // Last write wins: v=100 => available=900, locked=100.
        BalanceReadModelRow r = store.latestFor(7L, 0);
        assertEquals(900L, r.available());
        assertEquals(100L, r.locked());
        assertEquals(1L, writer.getRowsWritten());
        assertEquals(0, writer.getDirtyCount(), "cleared after a successful flush");
    }

    @Test
    void distinctKeysEachProduceOneRowInTheBatch() {
        writer.onBalanceChange(7L, 0, 100L, 0L);
        writer.onBalanceChange(7L, 1, 200L, 0L);
        writer.onBalanceChange(8L, 0, 300L, 0L);
        assertEquals(3, writer.getDirtyCount());

        assertEquals(3, writer.flushOnce());
        assertEquals(3, store.batches.get(0).size());
        assertEquals(3L, writer.getRowsWritten());
    }

    @Test
    void emptyFlushIsANoOp() {
        assertEquals(0, writer.flushOnce());
        assertTrue(store.batches.isEmpty());
        assertEquals(0L, writer.getRowsWritten());
        assertEquals(0L, writer.getFlushFailures());
    }

    @Test
    void failedFlushKeepsEverythingDirtyAndRetriesNextTick() {
        writer.onBalanceChange(7L, 0, 500L, 10L);
        store.fail = true;

        assertEquals(0, writer.flushOnce(), "failed flush wrote nothing");
        assertEquals(1L, writer.getFlushFailures());
        assertEquals(0L, writer.getRowsWritten());
        assertEquals(1, writer.getDirtyCount(), "dirt retained on failure");

        store.fail = false;
        assertEquals(1, writer.flushOnce(), "retry writes the retained row");
        assertEquals(1L, writer.getRowsWritten());
        assertEquals(0, writer.getDirtyCount());
        BalanceReadModelRow r = store.latestFor(7L, 0);
        assertEquals(500L, r.available());
        assertEquals(10L, r.locked());
    }

    @Test
    void aValueRefreshedMidFlushIsNotLost() {
        writer.onBalanceChange(7L, 0, 500L, 0L);
        // Simulate the poll thread publishing a newer value for the same key WHILE the upsert runs:
        // the value-checked remove must leave the fresh value dirty for the next tick (last-write-wins).
        store.onUpsert = () -> {
            store.onUpsert = null; // once
            writer.onBalanceChange(7L, 0, 999L, 5L);
        };

        assertEquals(1, writer.flushOnce());       // writes the 500 snapshot
        assertEquals(1, writer.getDirtyCount(), "the mid-flush 999 publish survived");

        store.onUpsert = null;
        assertEquals(1, writer.flushOnce());        // writes the newer 999
        BalanceReadModelRow r = store.latestFor(7L, 0);
        assertEquals(999L, r.available());
        assertEquals(5L, r.locked());
        assertEquals(0, writer.getDirtyCount());
    }

    @Test
    void stopDrainsRemainingDirtOnGracefulShutdown() {
        writer.start();
        try {
            writer.onBalanceChange(7L, 0, 42L, 7L);
            writer.stop(); // shuts the flush thread then does one final drain
            assertEquals(1, store.totalRows(), "stop() flushed the pending row");
            BalanceReadModelRow r = store.latestFor(7L, 0);
            assertEquals(42L, r.available());
            assertEquals(7L, r.locked());
            assertEquals(0, writer.getDirtyCount());
        } finally {
            // stop() already called; idempotent enough for the test's purposes.
        }
    }

    @Test
    void scheduledFlushLoopDrainsWithoutManualTicks() throws Exception {
        PgBalanceReadModelWriter fast = new PgBalanceReadModelWriter(store, 20); // 20ms cadence
        fast.start();
        try {
            fast.onBalanceChange(9L, 2, 123L, 4L);
            long deadline = System.currentTimeMillis() + 2_000;
            while (System.currentTimeMillis() < deadline && fast.getRowsWritten() == 0) {
                Thread.sleep(5);
            }
            assertEquals(1L, fast.getRowsWritten(), "the scheduled loop drained the dirty key");
            assertFalse(store.batches.isEmpty());
        } finally {
            fast.stop();
        }
    }
}
