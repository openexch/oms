// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

/**
 * A downstream consumer of the AE hold-snapshot stream, forwarded by {@link AeronAssetsBalanceStore}
 * (which is itself the {@link AssetsEgressListener}) rather than being attached as a second listener.
 *
 * <p>The Q3 orphan-hold reconciler implements this: it triggers a snapshot via
 * {@link AeronAssetsBalanceStore#requestHoldSnapshot(long)}, accumulates every
 * {@link #onHoldSnapshotEntry} until {@link #onHoldSnapshotEnd}, then diffs the AE's live holds
 * against OMS order state to find provably-never-submitted orphans.</p>
 *
 * <h3>Threading</h3>
 * <p>Both callbacks are invoked on the single {@code oms-assets-poll} thread (the store forwards them
 * straight from its egress callbacks). Implementations must not block or do heavy work inline. Entry
 * events carry no correlationId; {@link #onHoldSnapshotEnd} does, so the consumer correlates the
 * completed stream to the request it issued.</p>
 */
public interface HoldSnapshotConsumer {

    /**
     * One outstanding hold from the AE, mirroring {@link AssetsEgressListener#onHoldSnapshotEntry}.
     *
     * @param orderId   the order the hold belongs to (this is the OMS {@code omsOrderId})
     * @param userId    the account holding the funds
     * @param assetId   the held asset
     * @param remaining remaining held amount (8dp fixed-point)
     */
    void onHoldSnapshotEntry(long orderId, long userId, int assetId, long remaining);

    /**
     * Terminates the entry stream for one {@code RequestHoldSnapshot}.
     *
     * @param correlationId echoed from the originating request (correlate to the issued sweep)
     * @param entryCount    number of {@link #onHoldSnapshotEntry} events that preceded this terminator
     */
    void onHoldSnapshotEnd(long correlationId, int entryCount);
}
