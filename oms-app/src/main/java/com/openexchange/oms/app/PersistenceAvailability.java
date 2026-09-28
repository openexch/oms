// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/** Cached, expiring DB evidence. A health request never performs network I/O. */
final class PersistenceAvailability implements BooleanSupplier, Runnable {
    private final BooleanSupplier probe;
    private final LongSupplier clock;
    private volatile long lastSuccess = Long.MIN_VALUE;

    PersistenceAvailability(BooleanSupplier probe) { this(probe, System::nanoTime); }
    PersistenceAvailability(BooleanSupplier probe, LongSupplier clock) { this.probe = probe; this.clock = clock; }

    @Override public void run() {
        try { lastSuccess = probe.getAsBoolean() ? clock.getAsLong() : Long.MIN_VALUE; }
        catch (RuntimeException e) { lastSuccess = Long.MIN_VALUE; }
    }
    @Override public boolean getAsBoolean() {
        long observed = lastSuccess;
        return observed != Long.MIN_VALUE && clock.getAsLong() - observed < 3_000_000_000L;
    }
}
