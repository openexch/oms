// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Local read model of AE balances, maintained from the AE's egress on the single polling
 * thread and read from API threads.
 *
 * Self-correcting by construction: {@code BalanceUpdate} carries ABSOLUTE (available, locked)
 * values, so any transient local skew is overwritten by the next update for that (user, asset).
 * The only relative write is {@link #applyHoldDelta} — applied when a HoldAck arrives BEFORE its
 * waiting caller is released, so a client that just placed a hold reads its own hold immediately
 * (the following absolute BalanceUpdate then overwrites with identical values).
 *
 * Per-field reads are atomic; the (available, locked) PAIR may momentarily skew between two
 * updates — acceptable for display/pre-check reads (the authoritative gate is the AE hold).
 */
public final class BalanceProjection {

    private final int assetCount;
    private final ConcurrentHashMap<Long, AtomicLongArray> users = new ConcurrentHashMap<>();

    public BalanceProjection(final int assetCount) {
        this.assetCount = assetCount;
    }

    private AtomicLongArray row(final long userId) {
        return users.computeIfAbsent(userId, u -> new AtomicLongArray(2 * assetCount));
    }

    /** Absolute overwrite from a BalanceUpdate (poll thread). */
    public void set(final long userId, final int assetId, final long available, final long locked) {
        final AtomicLongArray row = row(userId);
        row.set(2 * assetId, available);
        row.set(2 * assetId + 1, locked);
    }

    /** Absolute available from a Deposit/WithdrawAck (locked untouched — those ops never lock). */
    public void setAvailable(final long userId, final int assetId, final long newAvailable) {
        row(userId).set(2 * assetId, newAvailable);
    }

    /** Read-your-hold: applied on HoldAck before the waiting caller is released. */
    public void applyHoldDelta(final long userId, final int assetId, final long amount) {
        final AtomicLongArray row = row(userId);
        row.addAndGet(2 * assetId, -amount);
        row.addAndGet(2 * assetId + 1, amount);
    }

    public long available(final long userId, final int assetId) {
        final AtomicLongArray row = users.get(userId);
        return row == null ? 0L : row.get(2 * assetId);
    }

    public long locked(final long userId, final int assetId) {
        final AtomicLongArray row = users.get(userId);
        return row == null ? 0L : row.get(2 * assetId + 1);
    }
}
