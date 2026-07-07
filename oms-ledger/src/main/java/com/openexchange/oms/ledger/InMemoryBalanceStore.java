// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.ledger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory implementation of {@link BalanceStore} for testing and standalone operation.
 * Uses ConcurrentHashMap with composite keys and per-user locking for atomicity.
 */
public class InMemoryBalanceStore implements BalanceStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryBalanceStore.class);

    private final ConcurrentHashMap<String, AtomicLong> balances = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Object> userLocks = new ConcurrentHashMap<>();
    private final Set<Long> processedTradeIds = ConcurrentHashMap.newKeySet();
    private final AtomicLong oversettleCount = new AtomicLong(0);

    private String availKey(long userId, int assetId) {
        return userId + ":" + assetId + ":avail";
    }

    private String lockedKey(long userId, int assetId) {
        return userId + ":" + assetId + ":locked";
    }

    private Object lockFor(long userId) {
        return userLocks.computeIfAbsent(userId, k -> new Object());
    }

    private AtomicLong getOrCreate(String key) {
        return balances.computeIfAbsent(key, k -> new AtomicLong(0));
    }

    @Override
    public long getAvailable(long userId, int assetId) {
        AtomicLong val = balances.get(availKey(userId, assetId));
        return val != null ? val.get() : 0L;
    }

    @Override
    public long getLocked(long userId, int assetId) {
        AtomicLong val = balances.get(lockedKey(userId, assetId));
        return val != null ? val.get() : 0L;
    }

    @Override
    public boolean hold(long userId, int assetId, long amount, long orderId) {
        if (amount <= 0) return false;
        synchronized (lockFor(userId)) {
            AtomicLong avail = getOrCreate(availKey(userId, assetId));
            if (avail.get() < amount) {
                return false;
            }
            avail.addAndGet(-amount);
            getOrCreate(lockedKey(userId, assetId)).addAndGet(amount);
            return true;
        }
    }

    @Override
    public boolean release(long userId, int assetId, long amount, long orderId) {
        if (amount <= 0) return false;
        synchronized (lockFor(userId)) {
            AtomicLong locked = getOrCreate(lockedKey(userId, assetId));
            if (locked.get() < amount) {
                return false;
            }
            locked.addAndGet(-amount);
            getOrCreate(availKey(userId, assetId)).addAndGet(amount);
            return true;
        }
    }

    @Override
    public boolean settle(long buyerUserId, long sellerUserId, int baseAssetId, int quoteAssetId,
                       long baseAmount, long quoteAmount, long tradeId) {
        if (!processedTradeIds.add(tradeId)) {
            log.debug("Duplicate trade settlement ignored: tradeId={}", tradeId);
            return false;
        }

        // Lock both users in consistent order to avoid deadlock
        long firstUser = Math.min(buyerUserId, sellerUserId);
        long secondUser = Math.max(buyerUserId, sellerUserId);

        synchronized (lockFor(firstUser)) {
            synchronized (lockFor(secondUser)) {
                // Floor-guarded debit of each locked leg (oms#84): locked must never go
                // negative. A short leg is recovered from that user's available balance
                // (bounded at 0) and reported as an over-settle. The credit legs are
                // applied unconditionally — the trade already happened on the cluster, so
                // the counterparty is always credited in full.
                long buyerShortfall = settleLockedLeg(buyerUserId, quoteAssetId, quoteAmount, tradeId, "buyer");
                long sellerShortfall = settleLockedLeg(sellerUserId, baseAssetId, baseAmount, tradeId, "seller");

                // Buyer receives base, seller receives quote (unconditional credits).
                getOrCreate(availKey(buyerUserId, baseAssetId)).addAndGet(baseAmount);
                getOrCreate(availKey(sellerUserId, quoteAssetId)).addAndGet(quoteAmount);

                if (buyerShortfall != 0 || sellerShortfall != 0) {
                    oversettleCount.incrementAndGet();
                }
            }
        }
        return true;
    }

    /**
     * Applies one locked-side settlement leg with a floor guard (oms#84).
     * <p>
     * Decrements the user's locked balance by {@code min(locked, amount)} so it can never
     * go negative. Any shortfall (the amount the locked pool could not cover) is pulled
     * from the same asset's available balance, bounded at 0. A nonzero shortfall means the
     * order settled for more than was ever held — an upstream accounting-invariant break —
     * and is logged at CRITICAL (log.error).
     * <p>
     * Must be called while holding {@code userId}'s lock.
     *
     * @return the shortfall (0 when the locked balance fully covered the leg)
     */
    private long settleLockedLeg(long userId, int assetId, long amount, long tradeId, String side) {
        AtomicLong locked = getOrCreate(lockedKey(userId, assetId));
        long lockedBefore = locked.get();
        long debit = Math.min(Math.max(lockedBefore, 0L), amount);
        locked.addAndGet(-debit);
        long shortfall = amount - debit;
        if (shortfall <= 0) {
            return 0L;
        }
        // Over-settle: locked held less than the trade requires. Recover from available
        // (bounded at 0); whatever cannot be recovered is real, unreconciled deficit.
        AtomicLong avail = getOrCreate(availKey(userId, assetId));
        long recovered = Math.min(Math.max(avail.get(), 0L), shortfall);
        avail.addAndGet(-recovered);
        long unrecovered = shortfall - recovered;
        log.error("CRITICAL over-settle — accounting invariant break: locked balance could not cover "
                + "settlement. side={}, userId={}, assetId={}, tradeId={}, requested={}, locked={}, "
                + "recoveredFromAvailable={}, unrecovered={}",
                side, userId, assetId, tradeId, amount, lockedBefore, recovered, unrecovered);
        return shortfall;
    }

    @Override
    public long getOversettleCount() {
        return oversettleCount.get();
    }

    @Override
    public void deposit(long userId, int assetId, long amount) {
        if (amount <= 0) throw new IllegalArgumentException("Deposit amount must be positive");
        getOrCreate(availKey(userId, assetId)).addAndGet(amount);
    }

    @Override
    public void withdraw(long userId, int assetId, long amount) {
        if (amount <= 0) throw new IllegalArgumentException("Withdraw amount must be positive");
        synchronized (lockFor(userId)) {
            AtomicLong avail = getOrCreate(availKey(userId, assetId));
            if (avail.get() < amount) {
                throw new IllegalStateException("Insufficient available balance for withdrawal");
            }
            avail.addAndGet(-amount);
        }
    }
}
