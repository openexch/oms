// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

/**
 * A lightweight change-tap on the OMS-side {@link BalanceProjection}, forwarded by
 * {@link AeronAssetsBalanceStore} (which is itself the {@link AssetsEgressListener}) rather than
 * being attached as a second listener. Mirrors the {@link HoldSnapshotConsumer} seam style.
 *
 * <p>The CQRS PG balance read-model writer implements this: every time the projection's absolute
 * {@code (available, locked)} for a {@code (user, asset)} changes on the poll thread, it is handed
 * the new absolutes and marks that key dirty for a batched, off-thread upsert into a durable,
 * queryable mirror. The mirror is for ops/analytics only; it is <b>never</b> money-authoritative
 * and is <b>never</b> read by the money path.</p>
 *
 * <h3>Threading</h3>
 * <p>{@link #onBalanceChange} is invoked on the single {@code oms-assets-poll} thread (the store
 * calls it straight from its egress callbacks). Implementations must not block or do heavy work
 * inline — do the minimum (record the latest value) and hand real work to another thread.</p>
 *
 * <h3>Value contract</h3>
 * <p>The two amounts are always the projection's ABSOLUTE post-change values for that
 * {@code (user, asset)} (8dp fixed-point), not deltas — so a consumer can apply last-write-wins
 * per key with no accumulation. For the read-your-hold delta case the store passes the projection's
 * current absolutes AFTER applying the delta.</p>
 */
@FunctionalInterface
public interface BalanceChangeConsumer {

    /**
     * The projection's absolute balance for one {@code (user, asset)} just changed.
     *
     * @param userId    the account
     * @param assetId   the asset
     * @param available available (unlocked) balance after the change, 8dp fixed-point
     * @param locked    locked (held) balance after the change, 8dp fixed-point
     */
    void onBalanceChange(long userId, int assetId, long available, long locked);
}
