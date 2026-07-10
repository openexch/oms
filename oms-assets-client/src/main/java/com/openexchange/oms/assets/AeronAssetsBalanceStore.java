// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

import com.openexchange.oms.ledger.BalanceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link BalanceStore} backed by the Assets Engine cluster — the OMS side of the money cutover.
 *
 * <p><b>HOLD is the synchronous gate</b> (same call shape as the Redis EVALSHA it replaces): the
 * caller blocks on a correlated future until HoldAck/HoldReject, bounded by {@code holdTimeoutMs}.
 * <b>Fail-CLOSED</b>: any timeout, disconnect, or queue-full rejects the order — a stale or absent
 * AE can only false-reject, never over-approve.</p>
 *
 * <p><b>The timeout compensator.</b> Blind hold retries are banned (a duplicate Hold is an atomic
 * top-up on the AE — retrying an ambiguous hold could double-lock). Instead, when a CREATE hold
 * times out with its outcome unknown, a {@code Release(orderId, -1)} is enqueued BEHIND the hold on
 * the same ordered ingress session: if the hold never applied the release is a gone-hold no-op; if
 * it applied late the release returns the full residual. Either way no orphaned lock survives. For
 * an AMEND delta (the base hold already acked — tracked in {@code ackedHoldOrderIds}) the {@code -1}
 * compensator would nuke the base hold, so it is SUPPRESSED: the worst case is a transient over-lock
 * (never money creation), cured by the order's terminal full-residual release, and counted.</p>
 *
 * <p><b>settle() moves no money.</b> The AE settles from the ME journal feed (the OMS is out of the
 * money path after submit); this method is only the LOCAL dedupe that gates the OMS's own side
 * effects (risk onFill, exec persist, applyFill): a strictly-increasing tradeId high-water,
 * initialized at boot from PG {@code max(trade_id)} and advanced on the single cluster-poll thread.</p>
 *
 * <p>Reads serve from the {@link BalanceProjection} (absolute, self-correcting, read-your-hold);
 * a pre-check false-accept is caught by the authoritative hold, a false-reject is transient.</p>
 */
public final class AeronAssetsBalanceStore implements BalanceStore, AssetsEgressListener {

    private static final Logger log = LoggerFactory.getLogger(AeronAssetsBalanceStore.class);

    /** Ack payload: accepted flag + reject reason code + newAvailable (deposit/withdraw acks). */
    private record Ack(boolean accepted, int reasonCode, long newAvailable) {
        static final Ack OK = new Ack(true, 0, 0);
    }

    private final AssetsTransport transport;
    private final BalanceProjection projection;
    private final long holdTimeoutMs;
    private final long ackTimeoutMs;

    private final ConcurrentHashMap<Long, CompletableFuture<Ack>> pending = new ConcurrentHashMap<>();
    /** corrId -> {orderId, userId} for pending CREATE holds, so a disconnect can compensate on reconnect. */
    private final ConcurrentHashMap<Long, long[]> pendingCreateHolds = new ConcurrentHashMap<>();
    /** Order ids with a live acked hold (populated from HoldAck; consulted by the compensator). */
    private final Set<Long> ackedHoldOrderIds = ConcurrentHashMap.newKeySet();
    /** orderId -> userId of create-holds whose outcome was unknown at disconnect; compensated on reconnect. */
    private final ConcurrentHashMap<Long, Long> compensateOnReconnect = new ConcurrentHashMap<>();

    private final AtomicLong correlationIds = new AtomicLong(ThreadLocalRandom.current().nextLong(1, 1L << 40));

    /** Settle-side-effect dedupe high-water; single-writer (the OMS cluster-poll thread). */
    private volatile long settleHighWater;
    private volatile boolean projectionReady;
    private volatile long snapshotCorrelationId;

    /**
     * Optional forwarding seam for the hold-snapshot stream (the Q3 orphan-hold reconciler). The
     * store stays the sole {@link AssetsEgressListener}; when set, this consumer is fed the same
     * {@code onHoldSnapshotEntry}/{@code onHoldSnapshotEnd} events on the poll thread. Volatile:
     * installed once at startup, read on the poll thread.
     */
    private volatile HoldSnapshotConsumer holdSnapshotConsumer;

    // Anomaly counters (single-writer or monotonic; scraped by metrics).
    private volatile long holdTimeouts;
    private volatile long amendOrphans;
    private volatile long lateAcks;
    private volatile long compensatorsSent;

    public AeronAssetsBalanceStore(final AssetsTransport transport, final int assetCount,
                                   final long holdTimeoutMs, final long ackTimeoutMs) {
        this.transport = transport;
        this.projection = new BalanceProjection(assetCount);
        this.holdTimeoutMs = holdTimeoutMs;
        this.ackTimeoutMs = ackTimeoutMs;
        transport.setEgressListener(this);
    }

    /** Boot-time init from PG max(trade_id): replaces the processed:{tradeId} cross-restart dedupe. */
    public void initSettleHighWater(final long maxAppliedTradeId) {
        this.settleHighWater = maxAppliedTradeId;
        log.info("settle high-water initialized to {}", maxAppliedTradeId);
    }

    public boolean isProjectionReady() {
        return projectionReady;
    }

    /**
     * Install the hold-snapshot forwarding seam (the orphan-hold reconciler). Idempotent; the last
     * consumer set wins. Passing {@code null} detaches. See {@link HoldSnapshotConsumer}.
     */
    public void setHoldSnapshotConsumer(final HoldSnapshotConsumer consumer) {
        this.holdSnapshotConsumer = consumer;
    }

    /**
     * Ask the AE to stream its outstanding holds (answered by {@code onHoldSnapshotEntry*} then
     * {@code onHoldSnapshotEnd}, forwarded to the {@link HoldSnapshotConsumer}). The reconciler owns
     * the correlationId; a {@code false} return means the request was back-pressured and no snapshot
     * will arrive for it.
     */
    public boolean requestHoldSnapshot(final long correlationId) {
        return transport.submitRequestHoldSnapshot(correlationId);
    }

    // ==================== BalanceStore ====================

    @Override
    public long getAvailable(final long userId, final int assetId) {
        return projection.available(userId, assetId);
    }

    @Override
    public long getLocked(final long userId, final int assetId) {
        return projection.locked(userId, assetId);
    }

    @Override
    public boolean hold(final long userId, final int assetId, final long amount, final long orderId) {
        return hold(userId, assetId, amount, orderId, false);
    }

    @Override
    public boolean hold(final long userId, final int assetId, final long amount, final long orderId,
                        final boolean omsManagedRelease) {
        if (amount <= 0 || !transport.isConnected()) {
            return false; // fail-closed; nothing was enqueued, no compensator needed
        }
        final boolean isAmendDelta = ackedHoldOrderIds.contains(orderId);
        final long corr = correlationIds.incrementAndGet();
        final CompletableFuture<Ack> future = new CompletableFuture<>();
        pending.put(corr, future);
        if (!isAmendDelta) {
            pendingCreateHolds.put(corr, new long[] {orderId, userId});
        }
        try {
            if (!transport.submitHold(corr, orderId, userId, assetId, amount, omsManagedRelease)) {
                return false; // queue full: command never left this process — safe plain reject
            }
            final Ack ack = future.get(holdTimeoutMs, TimeUnit.MILLISECONDS);
            return ack.accepted();
        } catch (TimeoutException e) {
            holdTimeouts = holdTimeouts + 1;
            if (isAmendDelta) {
                // A -1 release would nuke the acked base hold. Worst case: a transient over-lock
                // (never money creation), cured by the order's terminal full-residual release.
                amendOrphans = amendOrphans + 1;
                log.error("AMEND hold delta timed out (order={} corr={}): compensator suppressed; "
                        + "possible transient over-lock until terminal", orderId, corr);
            } else {
                sendCompensator(orderId, userId);
            }
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!isAmendDelta) {
                sendCompensator(orderId, userId);
            }
            return false;
        } catch (Exception e) {
            log.error("hold failed (order={} user={}): {}", orderId, userId, e.toString());
            return false;
        } finally {
            pending.remove(corr);
            pendingCreateHolds.remove(corr);
        }
    }

    /**
     * Enqueued BEHIND the (possibly still queued) hold on the same ordered session: the release
     * can never overtake it. Retried briefly on a full queue; a failure to enqueue is CRITICAL
     * (the orphan-hold reconciler is the backstop).
     */
    private void sendCompensator(final long orderId, final long userId) {
        for (int i = 0; i < 3; i++) {
            if (transport.submitRelease(orderId, userId, -1L)) {
                compensatorsSent = compensatorsSent + 1;
                return;
            }
            Thread.onSpinWait();
        }
        log.error("CRITICAL: compensating release for order {} could not be enqueued — "
                + "an orphaned hold may persist until the reconciler sweeps it", orderId);
    }

    @Override
    public boolean release(final long userId, final int assetId, final long amount, final long orderId) {
        if (amount <= 0) {
            return false;
        }
        return transport.submitRelease(orderId, userId, amount);
    }

    @Override
    public boolean releaseAll(final long userId, final int assetId, final long orderId) {
        ackedHoldOrderIds.remove(orderId);
        return transport.submitRelease(orderId, userId, -1L);
    }

    @Override
    public boolean supportsResidualHolds() {
        return true;
    }

    @Override
    public boolean settle(final long buyerUserId, final long sellerUserId, final int baseAssetId,
                          final int quoteAssetId, final long baseAmount, final long quoteAmount,
                          final long tradeId) {
        // Local dedupe only — the AE settles from the ME journal feed. Trades arrive in tradeId
        // order on the single poll thread; failover overlap re-delivers only <= high-water.
        if (tradeId <= settleHighWater) {
            return false;
        }
        settleHighWater = tradeId;
        return true;
    }

    @Override
    public void deposit(final long userId, final int assetId, final long amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("deposit amount must be positive: " + amount);
        }
        final Ack ack = roundTrip("deposit", corr -> transport.submitDeposit(corr, userId, assetId, amount));
        if (!ack.accepted()) {
            throw new IllegalStateException("deposit rejected: reason=" + ack.reasonCode());
        }
    }

    @Override
    public void withdraw(final long userId, final int assetId, final long amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("withdraw amount must be positive: " + amount);
        }
        final Ack ack = roundTrip("withdraw", corr -> transport.submitWithdraw(corr, userId, assetId, amount));
        if (!ack.accepted()) {
            // Interface contract: insufficient balance -> IllegalStateException.
            throw new IllegalStateException("withdraw rejected: reason=" + ack.reasonCode());
        }
    }

    private interface Submit {
        boolean submit(long correlationId);
    }

    /** Correlated round-trip for deposit/withdraw. Timeout = OUTCOME UNKNOWN — never auto-retry. */
    private Ack roundTrip(final String what, final Submit submit) {
        if (!transport.isConnected()) {
            throw new IllegalStateException(what + " failed: assets engine not connected");
        }
        final long corr = correlationIds.incrementAndGet();
        final CompletableFuture<Ack> future = new CompletableFuture<>();
        pending.put(corr, future);
        try {
            if (!submit.submit(corr)) {
                throw new IllegalStateException(what + " failed: command queue full");
            }
            return future.get(ackTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new IllegalStateException(what + " OUTCOME UNKNOWN: no ack within " + ackTimeoutMs
                    + "ms — do NOT retry blindly (the command may still apply)");
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IllegalStateException(what + " failed: " + e.getCause(), e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(what + " interrupted; outcome unknown");
        } finally {
            pending.remove(corr);
        }
    }

    // ==================== AssetsEgressListener (single poll thread) ====================

    @Override
    public void onHoldAck(final long correlationId, final long orderId, final long userId,
                          final int assetId, final long amount) {
        ackedHoldOrderIds.add(orderId);
        // Read-your-hold: the projection reflects the hold BEFORE the waiting caller is released.
        projection.applyHoldDelta(userId, assetId, amount);
        completePending(correlationId, Ack.OK);
    }

    @Override
    public void onHoldReject(final long correlationId, final long orderId, final long userId,
                             final int assetId, final long amount, final int reasonCode) {
        completePending(correlationId, new Ack(false, reasonCode, 0));
    }

    @Override
    public void onBalanceUpdate(final long userId, final int assetId, final long available, final long locked) {
        projection.set(userId, assetId, available, locked);
    }

    @Override
    public void onDepositAck(final long correlationId, final long userId, final int assetId,
                             final long amount, final long newAvailable) {
        projection.setAvailable(userId, assetId, newAvailable);
        completePending(correlationId, new Ack(true, 0, newAvailable));
    }

    @Override
    public void onWithdrawAck(final long correlationId, final long userId, final int assetId,
                              final long amount, final long newAvailable) {
        projection.setAvailable(userId, assetId, newAvailable);
        completePending(correlationId, new Ack(true, 0, newAvailable));
    }

    @Override
    public void onWithdrawReject(final long correlationId, final long userId, final int assetId,
                                 final long amount, final int reasonCode) {
        completePending(correlationId, new Ack(false, reasonCode, 0));
    }

    @Override
    public void onSettlementApplied(final long tradeId, final long buyerUserId, final long sellerUserId) {
        // The AE settled from the feed; balances arrive as absolute BalanceUpdates. Nothing local.
    }

    @Override
    public void onFeedPositionReport(final long correlationId, final long consumePosition,
                                     final long lastAppliedTradeId) {
        // Bridge-facing; the OMS store has no use for it.
    }

    @Override
    public void onBalanceSnapshotEnd(final long correlationId, final int entryCount) {
        if (correlationId == snapshotCorrelationId) {
            projectionReady = true;
            log.info("balance projection bootstrapped: {} entries", entryCount);
        }
    }

    @Override
    public void onHoldSnapshotEntry(final long orderId, final long userId, final int assetId, final long remaining) {
        // The store keeps its acked-hold set warm; the reconciler (if attached) consumes the stream.
        ackedHoldOrderIds.add(orderId);
        final HoldSnapshotConsumer consumer = holdSnapshotConsumer;
        if (consumer != null) {
            consumer.onHoldSnapshotEntry(orderId, userId, assetId, remaining);
        }
    }

    @Override
    public void onHoldSnapshotEnd(final long correlationId, final int entryCount) {
        final HoldSnapshotConsumer consumer = holdSnapshotConsumer;
        if (consumer != null) {
            consumer.onHoldSnapshotEnd(correlationId, entryCount);
        }
    }

    @Override
    public void onConnected() {
        requestProjectionBootstrap();
    }

    @Override
    public void onReconnected() {
        projectionReady = false;
        requestProjectionBootstrap();
        // Compensate create-holds whose outcome was unknown when the session died.
        compensateOnReconnect.forEach((orderId, userId) -> {
            if (transport.submitRelease(orderId, userId, -1L)) {
                compensateOnReconnect.remove(orderId);
                compensatorsSent = compensatorsSent + 1;
            }
        });
    }

    @Override
    public void onDisconnected() {
        projectionReady = false;
        // Fail-closed: every pending caller rejects; unknown create-holds get compensated later.
        pendingCreateHolds.forEach((corr, hold) -> compensateOnReconnect.put(hold[0], hold[1]));
        pending.forEach((corr, future) -> future.completeExceptionally(
                new IllegalStateException("assets engine session lost")));
    }

    private void requestProjectionBootstrap() {
        final long corr = correlationIds.incrementAndGet();
        snapshotCorrelationId = corr;
        if (!transport.submitRequestBalanceSnapshot(corr)) {
            log.error("balance snapshot request could not be enqueued; projection stays not-ready");
        }
    }

    private void completePending(final long correlationId, final Ack ack) {
        final CompletableFuture<Ack> future = pending.remove(correlationId);
        if (future != null) {
            future.complete(ack);
        } else if (correlationId != 0) {
            lateAcks = lateAcks + 1;
        }
    }

    // ==================== metrics accessors ====================

    public long getHoldTimeouts() {
        return holdTimeouts;
    }

    public long getAmendOrphans() {
        return amendOrphans;
    }

    public long getLateAcks() {
        return lateAcks;
    }

    public long getCompensatorsSent() {
        return compensatorsSent;
    }

    public long getSettleHighWater() {
        return settleHighWater;
    }
}
