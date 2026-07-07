// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.ledger;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Redis-backed implementation of {@link BalanceStore}.
 * <p>
 * Uses Lua scripts to guarantee atomicity of multi-key balance operations.
 * <p>
 * Redis key structure:
 * <ul>
 *   <li>{@code bal:{userId}} - HASH with fields {@code avail:{assetId}} and {@code locked:{assetId}}</li>
 *   <li>{@code holds:{userId}} - HASH mapping {@code orderId} to {@code "assetId:amount"}</li>
 *   <li>{@code processed:{tradeId}} - STRING "1" with TTL 1h for trade settlement idempotency</li>
 * </ul>
 */
public class RedisBalanceStore implements BalanceStore {

    private static final Logger log = LoggerFactory.getLogger(RedisBalanceStore.class);

    private static final String KEY_PREFIX_BAL = "bal:";
    private static final String KEY_PREFIX_HOLDS = "holds:";
    private static final String KEY_PREFIX_PROCESSED = "processed:";

    private static final String FIELD_AVAIL = "avail:";
    private static final String FIELD_LOCKED = "locked:";

    private static final long PROCESSED_TTL_SECONDS = 3600; // 1 hour

    // ----- Lua Scripts -----

    /**
     * KEYS[1] = bal:{userId}, KEYS[2] = holds:{userId}
     * ARGV[1] = avail field, ARGV[2] = locked field, ARGV[3] = amount, ARGV[4] = orderId, ARGV[5] = "assetId:amount"
     *
     * Returns 1 on success, 0 if insufficient available balance.
     */
    private static final String LUA_HOLD_BALANCE =
            "local avail = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0') " +
            "local amt = tonumber(ARGV[3]) " +
            "if avail < amt then " +
            "  return 0 " +
            "end " +
            "redis.call('HINCRBY', KEYS[1], ARGV[1], -amt) " +
            "redis.call('HINCRBY', KEYS[1], ARGV[2], amt) " +
            "redis.call('HSET', KEYS[2], ARGV[4], ARGV[5]) " +
            "return 1";

    /**
     * KEYS[1] = bal:{userId}, KEYS[2] = holds:{userId}
     * ARGV[1] = avail field, ARGV[2] = locked field, ARGV[3] = amount, ARGV[4] = orderId
     *
     * Returns 1 on success, 0 if insufficient locked balance.
     */
    private static final String LUA_RELEASE_BALANCE =
            "local locked = tonumber(redis.call('HGET', KEYS[1], ARGV[2]) or '0') " +
            "local amt = tonumber(ARGV[3]) " +
            "if locked < amt then " +
            "  return 0 " +
            "end " +
            "redis.call('HINCRBY', KEYS[1], ARGV[2], -amt) " +
            "redis.call('HINCRBY', KEYS[1], ARGV[1], amt) " +
            "redis.call('HDEL', KEYS[2], ARGV[4]) " +
            "return 1";

    /**
     * KEYS[1] = bal:{buyerUserId}, KEYS[2] = bal:{sellerUserId}, KEYS[3] = processed:{tradeId}
     * ARGV[1] = buyer locked quote field (locked:{quoteAssetId})
     * ARGV[2] = buyer avail base field (avail:{baseAssetId})
     * ARGV[3] = seller locked base field (locked:{baseAssetId})
     * ARGV[4] = seller avail quote field (avail:{quoteAssetId})
     * ARGV[5] = baseAmount
     * ARGV[6] = quoteAmount
     * ARGV[7] = TTL seconds
     * ARGV[8] = buyer avail quote field (avail:{quoteAssetId}) — for shortfall recovery
     * ARGV[9] = seller avail base field (avail:{baseAssetId})  — for shortfall recovery
     *
     * Floor-guarded settlement (oms#84): each locked leg is debited by at most what is
     * held so {@code locked} can never go negative; any shortfall is recovered from that
     * user's available balance of the same asset, bounded at 0. The credit legs are
     * unconditional — the counterparty is always credited in full.
     *
     * Returns a 3-element array {applied, buyerShortfall, sellerShortfall}:
     *   applied         = 1 on success, 0 if already processed (idempotent no-op)
     *   buyerShortfall  = quote the buyer's locked pool could not cover (0 = fully held)
     *   sellerShortfall = base the seller's locked pool could not cover (0 = fully held)
     */
    private static final String LUA_SETTLE_TRADE =
            "if redis.call('EXISTS', KEYS[3]) == 1 then " +
            "  return {0, 0, 0} " +
            "end " +
            "local baseAmt = tonumber(ARGV[5]) " +
            "local quoteAmt = tonumber(ARGV[6]) " +
            // Buyer locked-quote leg: debit min(locked, amount), recover shortfall from avail quote
            "local bLocked = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0') " +
            "local bDebit = bLocked " +
            "if bDebit > quoteAmt then bDebit = quoteAmt end " +
            "if bDebit < 0 then bDebit = 0 end " +
            "redis.call('HINCRBY', KEYS[1], ARGV[1], -bDebit) " +
            "local bShort = quoteAmt - bDebit " +
            "if bShort > 0 then " +
            "  local bAvail = tonumber(redis.call('HGET', KEYS[1], ARGV[8]) or '0') " +
            "  local bRec = bShort " +
            "  if bRec > bAvail then bRec = bAvail end " +
            "  if bRec < 0 then bRec = 0 end " +
            "  redis.call('HINCRBY', KEYS[1], ARGV[8], -bRec) " +
            "end " +
            // Seller locked-base leg: debit min(locked, amount), recover shortfall from avail base
            "local sLocked = tonumber(redis.call('HGET', KEYS[2], ARGV[3]) or '0') " +
            "local sDebit = sLocked " +
            "if sDebit > baseAmt then sDebit = baseAmt end " +
            "if sDebit < 0 then sDebit = 0 end " +
            "redis.call('HINCRBY', KEYS[2], ARGV[3], -sDebit) " +
            "local sShort = baseAmt - sDebit " +
            "if sShort > 0 then " +
            "  local sAvail = tonumber(redis.call('HGET', KEYS[2], ARGV[9]) or '0') " +
            "  local sRec = sShort " +
            "  if sRec > sAvail then sRec = sAvail end " +
            "  if sRec < 0 then sRec = 0 end " +
            "  redis.call('HINCRBY', KEYS[2], ARGV[9], -sRec) " +
            "end " +
            // Credit legs (unconditional): buyer receives base, seller receives quote
            "redis.call('HINCRBY', KEYS[1], ARGV[2], baseAmt) " +
            "redis.call('HINCRBY', KEYS[2], ARGV[4], quoteAmt) " +
            // Mark trade as processed
            "redis.call('SET', KEYS[3], '1', 'EX', tonumber(ARGV[7])) " +
            "return {1, bShort, sShort}";

    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisCommands<String, String> commands;

    private final String holdSha;
    private final String releaseSha;
    private final String settleSha;

    private final AtomicLong oversettleCount = new AtomicLong(0);

    public RedisBalanceStore(RedisClient redisClient) {
        this.redisClient = redisClient;
        this.connection = redisClient.connect();
        this.commands = connection.sync();

        // Pre-load Lua scripts into Redis and cache their SHA hashes for EVALSHA calls
        this.holdSha = commands.scriptLoad(LUA_HOLD_BALANCE);
        this.releaseSha = commands.scriptLoad(LUA_RELEASE_BALANCE);
        this.settleSha = commands.scriptLoad(LUA_SETTLE_TRADE);

        log.info("RedisBalanceStore initialized - hold={}, release={}, settle={}", holdSha, releaseSha, settleSha);
    }

    @Override
    public long getAvailable(long userId, int assetId) {
        String value = commands.hget(balKey(userId), FIELD_AVAIL + assetId);
        return value == null ? 0L : Long.parseLong(value);
    }

    @Override
    public long getLocked(long userId, int assetId) {
        String value = commands.hget(balKey(userId), FIELD_LOCKED + assetId);
        return value == null ? 0L : Long.parseLong(value);
    }

    @Override
    public boolean hold(long userId, int assetId, long amount, long orderId) {
        if (amount <= 0) {
            throw new IllegalArgumentException("Hold amount must be positive: " + amount);
        }

        String[] keys = {balKey(userId), holdsKey(userId)};
        String[] args = {
                FIELD_AVAIL + assetId,
                FIELD_LOCKED + assetId,
                Long.toString(amount),
                Long.toString(orderId),
                assetId + ":" + amount
        };

        Long result = commands.evalsha(holdSha, ScriptOutputType.INTEGER, keys, args);
        boolean success = result != null && result == 1L;

        if (success) {
            log.debug("Hold succeeded: userId={}, assetId={}, amount={}, orderId={}", userId, assetId, amount, orderId);
        } else {
            log.warn("Hold failed (insufficient balance): userId={}, assetId={}, amount={}, orderId={}", userId, assetId, amount, orderId);
        }

        return success;
    }

    @Override
    public boolean release(long userId, int assetId, long amount, long orderId) {
        if (amount <= 0) {
            throw new IllegalArgumentException("Release amount must be positive: " + amount);
        }

        String[] keys = {balKey(userId), holdsKey(userId)};
        String[] args = {
                FIELD_AVAIL + assetId,
                FIELD_LOCKED + assetId,
                Long.toString(amount),
                Long.toString(orderId)
        };

        Long result = commands.evalsha(releaseSha, ScriptOutputType.INTEGER, keys, args);
        boolean success = result != null && result == 1L;

        if (success) {
            log.debug("Release succeeded: userId={}, assetId={}, amount={}, orderId={}", userId, assetId, amount, orderId);
        } else {
            log.warn("Release failed (insufficient locked): userId={}, assetId={}, amount={}, orderId={}", userId, assetId, amount, orderId);
        }

        return success;
    }

    @Override
    public boolean settle(long buyerUserId, long sellerUserId, int baseAssetId, int quoteAssetId,
                       long baseAmount, long quoteAmount, long tradeId) {
        if (baseAmount <= 0 || quoteAmount <= 0) {
            throw new IllegalArgumentException(
                    "Settlement amounts must be positive: baseAmount=" + baseAmount + ", quoteAmount=" + quoteAmount);
        }

        String[] keys = {
                balKey(buyerUserId),
                balKey(sellerUserId),
                processedKey(tradeId)
        };
        String[] args = {
                FIELD_LOCKED + quoteAssetId,    // buyer locked quote field
                FIELD_AVAIL + baseAssetId,      // buyer avail base field
                FIELD_LOCKED + baseAssetId,     // seller locked base field
                FIELD_AVAIL + quoteAssetId,     // seller avail quote field
                Long.toString(baseAmount),
                Long.toString(quoteAmount),
                Long.toString(PROCESSED_TTL_SECONDS),
                FIELD_AVAIL + quoteAssetId,     // buyer avail quote field (shortfall recovery)
                FIELD_AVAIL + baseAssetId       // seller avail base field (shortfall recovery)
        };

        List<Object> result = commands.evalsha(settleSha, ScriptOutputType.MULTI, keys, args);
        long applied = resultAt(result, 0);
        long buyerShortfall = resultAt(result, 1);
        long sellerShortfall = resultAt(result, 2);

        if (applied != 1L) {
            log.info("Trade already processed (idempotent no-op): tradeId={}", tradeId);
            return false;
        }

        // Over-settle floor guard (oms#84): a nonzero shortfall means a locked pool held
        // less than the trade required — an upstream accounting-invariant break. The Lua
        // already clamped + recovered from available; here we raise it LOUD.
        if (buyerShortfall != 0 || sellerShortfall != 0) {
            oversettleCount.incrementAndGet();
            if (buyerShortfall != 0) {
                log.error("CRITICAL over-settle — accounting invariant break: buyer locked quote could not "
                        + "cover settlement. userId={}, assetId={}, tradeId={}, requested={}, shortfall={}",
                        buyerUserId, quoteAssetId, tradeId, quoteAmount, buyerShortfall);
            }
            if (sellerShortfall != 0) {
                log.error("CRITICAL over-settle — accounting invariant break: seller locked base could not "
                        + "cover settlement. userId={}, assetId={}, tradeId={}, requested={}, shortfall={}",
                        sellerUserId, baseAssetId, tradeId, baseAmount, sellerShortfall);
            }
        }

        log.debug("Trade settled: tradeId={}, buyer={}, seller={}, base={}:{}, quote={}:{}",
                tradeId, buyerUserId, sellerUserId, baseAssetId, baseAmount, quoteAssetId, quoteAmount);
        return true;
    }

    @Override
    public long getOversettleCount() {
        return oversettleCount.get();
    }

    /**
     * Reads the element at {@code index} from a Lua MULTI reply as a long.
     * Redis integer replies arrive as {@link Long}; anything else (or a short/absent
     * list) is treated as 0.
     */
    private static long resultAt(List<Object> result, int index) {
        if (result == null || index >= result.size()) {
            return 0L;
        }
        Object v = result.get(index);
        return v instanceof Number ? ((Number) v).longValue() : 0L;
    }

    @Override
    public void deposit(long userId, int assetId, long amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("Deposit amount must be positive: " + amount);
        }

        commands.hincrby(balKey(userId), FIELD_AVAIL + assetId, amount);
        log.debug("Deposit: userId={}, assetId={}, amount={}", userId, assetId, amount);
    }

    @Override
    public void withdraw(long userId, int assetId, long amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("Withdraw amount must be positive: " + amount);
        }

        long available = getAvailable(userId, assetId);
        if (available < amount) {
            throw new IllegalStateException(
                    "Insufficient available balance for withdrawal: userId=" + userId +
                    ", assetId=" + assetId + ", available=" + available + ", requested=" + amount);
        }

        commands.hincrby(balKey(userId), FIELD_AVAIL + assetId, -amount);
        log.debug("Withdraw: userId={}, assetId={}, amount={}", userId, assetId, amount);
    }

    /**
     * Closes the underlying Redis connection. Call on shutdown.
     */
    public void close() {
        connection.close();
        log.info("RedisBalanceStore connection closed");
    }

    // ----- Key builders -----

    private static String balKey(long userId) {
        return KEY_PREFIX_BAL + userId;
    }

    private static String holdsKey(long userId) {
        return KEY_PREFIX_HOLDS + userId;
    }

    private static String processedKey(long tradeId) {
        return KEY_PREFIX_PROCESSED + tradeId;
    }
}
