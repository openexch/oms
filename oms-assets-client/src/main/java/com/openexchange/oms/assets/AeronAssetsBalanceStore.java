// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

import com.openexchange.oms.ledger.BalanceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * A confirmed reject or failure to enqueue returns false. After enqueue, timeout,
 * interruption and session loss mean OUTCOME UNKNOWN and throw. The caller must
 * retain its durable intent and close admission until authoritative recovery.
 * No automatic retry or release is issued: an existing hold may belong to a
 * live ME order, including an amend whose base hold predates this OMS process.</p>
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
    private final AtomicLong correlationIds = new AtomicLong(ThreadLocalRandom.current().nextLong(1, 1L << 40));

    /** Settle-side-effect dedupe high-water; single-writer (the OMS cluster-poll thread). */
    private volatile long settleHighWater;
    /**
     * Out-of-order settle dedupe (the 2026-07-11 gap storm). tradeId is GLOBAL-dense but each
     * market flushes egress on its own timer into one queue, so cross-market arrival is routinely
     * a few ids out of global order. The old {@code tradeId <= settleHighWater} check silently
     * swallowed every late trade (~16% of all fills: no execution row, no fill accounting).
     * Now: everything at or below {@code settleFloor} is a known-applied duplicate (boot floor
     * from PG max(trade_id); failover overlap); above the floor an explicit applied-id set
     * dedupes, so late arrivals within {@link #SETTLE_WINDOW} ids of the high-water APPLY.
     * Single-writer (the OMS cluster-poll thread), like the fields above.
     */
    private static final long SETTLE_WINDOW = 8_192;
    private long settleFloor;
    private final org.agrona.collections.LongHashSet settledAboveFloor =
            new org.agrona.collections.LongHashSet();
    private volatile boolean projectionReady;
    private volatile long snapshotCorrelationId;

    /**
     * Optional forwarding seam for the hold-snapshot stream (the Q3 orphan-hold reconciler). The
     * store stays the sole {@link AssetsEgressListener}; when set, this consumer is fed the same
     * {@code onHoldSnapshotEntry}/{@code onHoldSnapshotEnd} events on the poll thread. Volatile:
     * installed once at startup, read on the poll thread.
     */
    private volatile HoldSnapshotConsumer holdSnapshotConsumer;

    /**
     * Optional change-tap on the balance projection (the CQRS PG read-model writer). Fed the absolute
     * post-change {@code (available, locked)} for every {@code (user, asset)} that mutates on the poll
     * thread. Volatile: installed once at startup, read on the poll thread. Zero work when unset.
     */
    private volatile BalanceChangeConsumer balanceChangeConsumer;

    // Anomaly counters (single-writer or monotonic; scraped by metrics).
    private volatile long holdTimeouts;
    private volatile long lateAcks;

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
        this.settleFloor = maxAppliedTradeId;
        this.settledAboveFloor.clear();
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
     * Install the balance change-tap (the CQRS PG read-model writer). Idempotent; the last consumer
     * set wins. Passing {@code null} detaches. See {@link BalanceChangeConsumer}. Zero cost when
     * unset (a single volatile null check on the poll thread).
     */
    public void setBalanceChangeConsumer(final BalanceChangeConsumer consumer) {
        this.balanceChangeConsumer = consumer;
    }

    /** Forward the projection's absolute post-change values to the change-tap, if one is attached. */
    private void fireBalanceChange(final long userId, final int assetId, final long available,
                                   final long locked) {
        final BalanceChangeConsumer consumer = balanceChangeConsumer;
        if (consumer != null) {
            consumer.onBalanceChange(userId, assetId, available, locked);
        }
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
        final long corr = correlationIds.incrementAndGet();
        final CompletableFuture<Ack> future = new CompletableFuture<>();
        pending.put(corr, future);
        try {
            if (!transport.submitHold(corr, orderId, userId, assetId, amount, omsManagedRelease)) {
                return false; // Command never left this process.
            }
            return future.get(holdTimeoutMs, TimeUnit.MILLISECONDS).accepted();
        } catch (TimeoutException e) {
            holdTimeouts = holdTimeouts + 1;
            throw new IllegalStateException("hold OUTCOME UNKNOWN for order " + orderId + ": acknowledgement timed out", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("hold OUTCOME UNKNOWN for order " + orderId + ": interrupted", e);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IllegalStateException("hold OUTCOME UNKNOWN for order " + orderId + ": session lost", e.getCause());
        } finally {
            pending.remove(corr);
        }
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
        // Local dedupe only — the AE settles from the ME journal feed, so nothing is sent here.
        // NOT a high-water check: trades arrive slightly out of global-tradeId order by design
        // (per-market flush timers), and a high-water dedupe swallowed every late fill (the
        // 2026-07-11 gap storm). Floor + applied-set instead: duplicates (failover overlap,
        // boot-floor history) still return false; late-but-new trades apply.
        if (tradeId <= settleFloor || !settledAboveFloor.add(tradeId)) {
            return false;
        }
        if (tradeId > settleHighWater) {
            settleHighWater = tradeId;
        }
        // Advance the floor lazily so the set stays bounded: everything more than SETTLE_WINDOW
        // ids behind the high-water is by then either applied or abandoned (a >window-late trade
        // is treated as a duplicate — same swallow as before, but now only past ~7 minutes of
        // lateness instead of 20 milliseconds).
        if (settleHighWater - settleFloor > SETTLE_WINDOW * 2) {
            final long newFloor = settleHighWater - SETTLE_WINDOW;
            settledAboveFloor.removeIfLong(id -> id <= newFloor);
            settleFloor = newFloor;
        }
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
        // Read-your-hold: the projection reflects the hold BEFORE the waiting caller is released.
        projection.applyHoldDelta(userId, assetId, amount);
        // Change-tap: the delta case forwards the projection's current absolutes AFTER applying.
        fireBalanceChange(userId, assetId, projection.available(userId, assetId),
                projection.locked(userId, assetId));
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
        // Absolute values straight from the AE — forward them verbatim to the change-tap.
        fireBalanceChange(userId, assetId, available, locked);
    }

    @Override
    public void onDepositAck(final long correlationId, final long userId, final int assetId,
                             final long amount, final long newAvailable) {
        projection.setAvailable(userId, assetId, newAvailable);
        // Deposit never touches locked; forward the new available with the projection's current locked.
        fireBalanceChange(userId, assetId, newAvailable, projection.locked(userId, assetId));
        completePending(correlationId, new Ack(true, 0, newAvailable));
    }

    @Override
    public void onWithdrawAck(final long correlationId, final long userId, final int assetId,
                              final long amount, final long newAvailable) {
        projection.setAvailable(userId, assetId, newAvailable);
        // Withdraw never touches locked; forward the new available with the projection's current locked.
        fireBalanceChange(userId, assetId, newAvailable, projection.locked(userId, assetId));
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
        // Read-only discrepancy/recovery consumer, if attached.
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

    }

    @Override
    public void onDisconnected() {
        projectionReady = false;
        // Every enqueued caller retains an unknown outcome. Reconnect cannot
        // authorize a financial compensator without durable command history.
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
        return 0; // Legacy metric retained; process-local amend classification was removed.
    }

    public long getLateAcks() {
        return lateAcks;
    }

    public long getCompensatorsSent() {
        return 0; // Automatic financial compensation is no longer performed.
    }

    public long getSettleHighWater() {
        return settleHighWater;
    }
}
