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
    public void settle(long buyerUserId, long sellerUserId, int baseAssetId, int quoteAssetId,
                       long baseAmount, long quoteAmount, long tradeId) {
        if (!processedTradeIds.add(tradeId)) {
            log.debug("Duplicate trade settlement ignored: tradeId={}", tradeId);
            return;
        }

        // Lock both users in consistent order to avoid deadlock
        long firstUser = Math.min(buyerUserId, sellerUserId);
        long secondUser = Math.max(buyerUserId, sellerUserId);

        synchronized (lockFor(firstUser)) {
            synchronized (lockFor(secondUser)) {
                // Buyer: locked quote decreases, available base increases
                getOrCreate(lockedKey(buyerUserId, quoteAssetId)).addAndGet(-quoteAmount);
                getOrCreate(availKey(buyerUserId, baseAssetId)).addAndGet(baseAmount);

                // Seller: locked base decreases, available quote increases
                getOrCreate(lockedKey(sellerUserId, baseAssetId)).addAndGet(-baseAmount);
                getOrCreate(availKey(sellerUserId, quoteAssetId)).addAndGet(quoteAmount);
            }
        }
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
