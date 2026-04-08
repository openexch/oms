package com.openexchange.oms.risk;

import com.match.domain.FixedPoint;
import com.openexchange.oms.common.domain.Market;
import com.openexchange.oms.common.domain.RiskRejectReason;
import com.openexchange.oms.common.enums.OmsOrderType;
import com.openexchange.oms.common.enums.OrderSide;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pre-trade risk engine executing 7 ordered checks in a fail-fast pipeline.
 * <p>
 * All data structures are in-memory using primitive arrays and Agrona off-heap maps
 * to avoid object allocation on the hot path. Target latency is &lt;100 microseconds
 * for the full check pipeline.
 *
 * <h3>Check pipeline (fail-fast order):</h3>
 * <ol>
 *   <li><b>Rate Limiter</b> — sliding window counters per user (orders/sec + orders/min)</li>
 *   <li><b>Circuit Breaker</b> — per-market halt if price moved &gt;N% in window</li>
 *   <li><b>Order Size Limits</b> — per-market min/max quantity and min/max notional</li>
 *   <li><b>Price Collar</b> — reject limit orders deviating &gt;N% from last trade price</li>
 *   <li><b>Open Order Limit</b> — max open orders per user</li>
 *   <li><b>Balance Sufficiency</b> — delegated to a {@link BalanceChecker}</li>
 *   <li><b>Position Limits</b> — max position per user per market</li>
 * </ol>
 *
 * <p>Thread-safety: a single risk engine instance must be called from a single thread
 * (the order processing thread). Market data and circuit breaker state may be updated
 * from other threads via volatile writes.</p>
 */
public final class RiskEngine {

    private static final Logger log = LoggerFactory.getLogger(RiskEngine.class);

    /** Sentinel value for Agrona Long2LongHashMap indicating missing entry. */
    private static final long MISSING_VALUE = Long.MIN_VALUE;

    /** Fixed percentage scale: 100 in fixed-point for percentage calculations. */
    private static final long HUNDRED_FP = FixedPoint.fromDouble(100.0);

    // -- Configuration --
    /** Per-market risk config. Array indexed by marketId. Null entry = market not configured. */
    private final RiskConfig[] marketConfigs;
    private final int maxMarkets;

    // -- External providers --
    private final MarketDataProvider marketDataProvider;
    private final BalanceChecker balanceChecker;

    // -- Check 1: Rate limiter state --
    // Sliding window implemented as circular buffer of timestamps per user.
    // Key: userId -> RateLimitState
    private final Long2ObjectHashMap<RateLimitState> rateLimitStates;

    // -- Check 2: Circuit breaker state --
    // Boolean array indexed by marketId. Set externally via tripCircuitBreaker/resetCircuitBreaker.
    // The array reference is final; individual elements are written by external threads and read
    // by the risk-check thread. Visibility is guaranteed by the happens-before from the volatile
    // circuitBreakerGeneration fence.
    private final boolean[] circuitBreakerTripped;
    @SuppressWarnings("unused")
    private volatile long circuitBreakerGeneration;

    // -- Check 5: Open order counts --
    // Key: userId -> open order count (stored as long). Updated via onOrderOpened/onOrderClosed.
    private final Long2LongHashMap openOrderCounts;

    // -- Check 7: Position tracking --
    // Composite key: encodePositionKey(userId, marketId) -> signed position in fixed-point.
    // Positive = long, negative = short.
    private final Long2ObjectHashMap<long[]> userPositions;

    /**
     * Internal sliding-window rate limit state per user.
     * Uses two circular timestamp buffers (second-granularity and minute-granularity).
     */
    private static final class RateLimitState {
        /** Circular buffer of order timestamps (epoch millis) — sized for per-second window. */
        final long[] secTimestamps;
        int secHead;
        int secCount;

        /** Circular buffer of order timestamps (epoch millis) — sized for per-minute window. */
        final long[] minTimestamps;
        int minHead;
        int minCount;

        RateLimitState(int maxPerSec, int maxPerMin) {
            this.secTimestamps = new long[maxPerSec + 1];
            this.secHead = 0;
            this.secCount = 0;
            this.minTimestamps = new long[maxPerMin + 1];
            this.minHead = 0;
            this.minCount = 0;
        }
    }

    public RiskEngine(int maxMarkets,
                      MarketDataProvider marketDataProvider,
                      BalanceChecker balanceChecker) {
        this.maxMarkets = maxMarkets;
        this.marketConfigs = new RiskConfig[maxMarkets];
        this.marketDataProvider = marketDataProvider;
        this.balanceChecker = balanceChecker;

        this.rateLimitStates = new Long2ObjectHashMap<>();
        this.circuitBreakerTripped = new boolean[maxMarkets];
        this.openOrderCounts = new Long2LongHashMap(MISSING_VALUE);
        this.userPositions = new Long2ObjectHashMap<>();
    }

    // -- Configuration --

    /**
     * Set or update the risk configuration for a market.
     * Must be called before any checks on that market.
     */
    public void setMarketConfig(int marketId, RiskConfig config) {
        if (marketId < 0 || marketId >= maxMarkets) {
            throw new IllegalArgumentException("marketId out of range: " + marketId);
        }
        marketConfigs[marketId] = config;
    }

    // -- Main check pipeline --

    /**
     * Run the full pre-trade risk check pipeline.
     *
     * @param userId   user placing the order
     * @param marketId market for the order
     * @param side     buy or sell
     * @param type     order type
     * @param price    limit price in fixed-point (0 for market orders)
     * @param quantity order quantity in fixed-point
     * @return {@link RiskResult#PASS} or a rejection result with reason
     */
    public RiskResult check(long userId, int marketId, OrderSide side,
                            OmsOrderType type, long price, long quantity) {

        final RiskConfig config = getConfig(marketId);
        if (config == null) {
            return RiskResult.reject(RiskRejectReason.INVALID_ORDER);
        }

        // Capture current time once — avoid repeated System.currentTimeMillis calls
        final long nowMs = System.currentTimeMillis();

        // 1. Rate limiter
        RiskResult result = checkRateLimit(userId, config, nowMs);
        if (!result.isPassed()) {
            return result;
        }

        // 2. Circuit breaker
        result = checkCircuitBreaker(marketId);
        if (!result.isPassed()) {
            return result;
        }

        // 3. Order size limits
        result = checkOrderSize(config, price, quantity, type, marketId);
        if (!result.isPassed()) {
            return result;
        }

        // 4. Price collar
        result = checkPriceCollar(config, marketId, type, price);
        if (!result.isPassed()) {
            return result;
        }

        // 5. Open order limit
        result = checkOpenOrderLimit(userId, config);
        if (!result.isPassed()) {
            return result;
        }

        // 6. Balance sufficiency
        result = checkBalance(userId, marketId, side, price, quantity, type);
        if (!result.isPassed()) {
            return result;
        }

        // 7. Position limits
        result = checkPositionLimit(userId, marketId, side, quantity, config);
        if (!result.isPassed()) {
            return result;
        }

        return RiskResult.PASS;
    }

    // -- Check 1: Rate Limiter --

    private RiskResult checkRateLimit(long userId, RiskConfig config, long nowMs) {
        RateLimitState state = rateLimitStates.get(userId);
        if (state == null) {
            state = new RateLimitState(config.getMaxOrdersPerSec(), config.getMaxOrdersPerMin());
            rateLimitStates.put(userId, state);
        }

        final int maxPerSec = config.getMaxOrdersPerSec();
        final int maxPerMin = config.getMaxOrdersPerMin();
        final long oneSecAgo = nowMs - 1_000L;
        final long oneMinAgo = nowMs - 60_000L;

        // Evict expired entries from second window
        while (state.secCount > 0) {
            int tailIdx = (state.secHead - state.secCount + state.secTimestamps.length)
                    % state.secTimestamps.length;
            if (state.secTimestamps[tailIdx] < oneSecAgo) {
                state.secCount--;
            } else {
                break;
            }
        }

        // Evict expired entries from minute window
        while (state.minCount > 0) {
            int tailIdx = (state.minHead - state.minCount + state.minTimestamps.length)
                    % state.minTimestamps.length;
            if (state.minTimestamps[tailIdx] < oneMinAgo) {
                state.minCount--;
            } else {
                break;
            }
        }

        // Check limits BEFORE recording (we reject if adding this order would exceed)
        if (state.secCount >= maxPerSec) {
            if (log.isDebugEnabled()) {
                log.debug("Rate limit (sec) exceeded for user {}: {}/{}", userId, state.secCount, maxPerSec);
            }
            return RiskResult.reject(RiskRejectReason.RATE_LIMIT_EXCEEDED);
        }
        if (state.minCount >= maxPerMin) {
            if (log.isDebugEnabled()) {
                log.debug("Rate limit (min) exceeded for user {}: {}/{}", userId, state.minCount, maxPerMin);
            }
            return RiskResult.reject(RiskRejectReason.RATE_LIMIT_EXCEEDED);
        }

        // Record this order timestamp
        state.secTimestamps[state.secHead] = nowMs;
        state.secHead = (state.secHead + 1) % state.secTimestamps.length;
        state.secCount++;

        state.minTimestamps[state.minHead] = nowMs;
        state.minHead = (state.minHead + 1) % state.minTimestamps.length;
        state.minCount++;

        return RiskResult.PASS;
    }

    // -- Check 2: Circuit Breaker --

    private RiskResult checkCircuitBreaker(int marketId) {
        // Volatile read of generation counter establishes happens-before with writer
        @SuppressWarnings("unused")
        long gen = circuitBreakerGeneration;
        if (circuitBreakerTripped[marketId]) {
            return RiskResult.reject(RiskRejectReason.CIRCUIT_BREAKER_OPEN);
        }
        return RiskResult.PASS;
    }

    /**
     * Trip the circuit breaker for a market. Called externally when price
     * move exceeds the configured threshold within the observation window.
     */
    public void tripCircuitBreaker(int marketId) {
        circuitBreakerTripped[marketId] = true;
        circuitBreakerGeneration++; // volatile write — publishes the array element store
        log.warn("Circuit breaker tripped for market {}", marketId);
    }

    /**
     * Reset the circuit breaker for a market, allowing orders to flow again.
     */
    public void resetCircuitBreaker(int marketId) {
        circuitBreakerTripped[marketId] = false;
        circuitBreakerGeneration++; // volatile write — publishes the array element store
        log.info("Circuit breaker reset for market {}", marketId);
    }

    /**
     * Query whether the circuit breaker is currently tripped for a market.
     */
    public boolean isCircuitBreakerTripped(int marketId) {
        return circuitBreakerTripped[marketId];
    }

    // -- Check 3: Order Size Limits --

    private RiskResult checkOrderSize(RiskConfig config, long price, long quantity,
                                      OmsOrderType type, int marketId) {
        // Quantity bounds
        if (quantity < config.getMinQuantity()) {
            return RiskResult.reject(RiskRejectReason.ORDER_SIZE_TOO_SMALL);
        }
        if (quantity > config.getMaxQuantity()) {
            return RiskResult.reject(RiskRejectReason.ORDER_SIZE_TOO_LARGE);
        }

        // Notional bounds — for market orders, use last trade price as estimate
        final long effectivePrice;
        if (type == OmsOrderType.MARKET || price == 0L) {
            effectivePrice = marketDataProvider.getLastTradePrice(marketId);
            if (effectivePrice == 0L) {
                // No reference price available — skip notional check for market orders
                return RiskResult.PASS;
            }
        } else {
            effectivePrice = price;
        }

        final long notional = FixedPoint.multiply(effectivePrice, quantity);

        if (notional < config.getMinNotional()) {
            return RiskResult.reject(RiskRejectReason.NOTIONAL_TOO_SMALL);
        }
        if (notional > config.getMaxNotional()) {
            return RiskResult.reject(RiskRejectReason.NOTIONAL_TOO_LARGE);
        }

        return RiskResult.PASS;
    }

    // -- Check 4: Price Collar --

    private RiskResult checkPriceCollar(RiskConfig config, int marketId,
                                        OmsOrderType type, long price) {
        // Price collar only applies to limit orders with an explicit price
        if (type == OmsOrderType.MARKET || price == 0L) {
            return RiskResult.PASS;
        }

        final long lastTradePrice = marketDataProvider.getLastTradePrice(marketId);
        if (lastTradePrice == 0L) {
            // No reference price — cannot apply collar, allow the order
            return RiskResult.PASS;
        }

        final int collarPct = config.getPriceCollarPercent();
        if (collarPct <= 0) {
            return RiskResult.PASS;
        }

        // Compute allowed deviation: lastTradePrice * collarPct / 100
        // Using integer arithmetic to avoid FixedPoint division:
        // maxDeviation = lastTradePrice * collarPct / 100
        final long maxDeviation = lastTradePrice / 100L * collarPct;

        final long diff = price > lastTradePrice ? price - lastTradePrice : lastTradePrice - price;

        if (diff > maxDeviation) {
            if (log.isDebugEnabled()) {
                log.debug("Price collar breach: price={} lastTrade={} maxDev={} actual={}",
                        FixedPoint.format(price), FixedPoint.format(lastTradePrice),
                        FixedPoint.format(maxDeviation), FixedPoint.format(diff));
            }
            return RiskResult.reject(RiskRejectReason.PRICE_COLLAR_BREACH);
        }

        return RiskResult.PASS;
    }

    // -- Check 5: Open Order Limit --

    private RiskResult checkOpenOrderLimit(long userId, RiskConfig config) {
        final long current = openOrderCounts.get(userId);
        final long count = (current == MISSING_VALUE) ? 0L : current;
        if (count >= config.getMaxOpenOrders()) {
            return RiskResult.reject(RiskRejectReason.OPEN_ORDER_LIMIT);
        }
        return RiskResult.PASS;
    }

    /**
     * Increment open order count for a user. Call when an order is placed or accepted.
     */
    public void onOrderOpened(long userId) {
        final long current = openOrderCounts.get(userId);
        openOrderCounts.put(userId, (current == MISSING_VALUE) ? 1L : current + 1L);
    }

    /**
     * Decrement open order count for a user. Call when an order is filled, cancelled, or expired.
     */
    public void onOrderClosed(long userId) {
        final long current = openOrderCounts.get(userId);
        if (current != MISSING_VALUE && current > 0L) {
            openOrderCounts.put(userId, current - 1L);
        }
    }

    // -- Check 6: Balance Sufficiency --

    private RiskResult checkBalance(long userId, int marketId, OrderSide side,
                                    long price, long quantity, OmsOrderType type) {
        // For buys: need quote asset (notional = price * quantity)
        // For sells: need base asset (quantity)
        // Asset IDs are derived from the Market definition.

        final Market market;
        try {
            market = Market.fromId(marketId);
        } catch (IllegalArgumentException e) {
            return RiskResult.reject(RiskRejectReason.INVALID_ORDER);
        }

        final long amount;
        final int assetId;

        if (side == OrderSide.BUY) {
            final long effectivePrice;
            if (type == OmsOrderType.MARKET || price == 0L) {
                // Use best ask as upper-bound estimate for market buy
                effectivePrice = marketDataProvider.getBestAsk(marketId);
                if (effectivePrice == 0L) {
                    // No ask available — cannot estimate cost, let balance checker decide
                    return RiskResult.PASS;
                }
            } else {
                effectivePrice = price;
            }
            amount = FixedPoint.multiply(effectivePrice, quantity);
            assetId = market.quoteAsset().id();
        } else {
            amount = quantity;
            assetId = market.baseAsset().id();
        }

        if (!balanceChecker.hasSufficientBalance(userId, assetId, amount)) {
            return RiskResult.reject(RiskRejectReason.INSUFFICIENT_BALANCE);
        }
        return RiskResult.PASS;
    }

    // -- Check 7: Position Limits --

    private RiskResult checkPositionLimit(long userId, int marketId, OrderSide side,
                                          long quantity, RiskConfig config) {
        final long maxPosition = config.getMaxPositionPerMarket();
        if (maxPosition == Long.MAX_VALUE) {
            // No position limit configured
            return RiskResult.PASS;
        }

        long[] positions = userPositions.get(userId);
        final long currentPosition;
        if (positions == null || marketId >= positions.length) {
            currentPosition = 0L;
        } else {
            currentPosition = positions[marketId];
        }

        // Compute new position after this order would fully fill
        final long newPosition;
        if (side == OrderSide.BUY) {
            newPosition = currentPosition + quantity;
        } else {
            newPosition = currentPosition - quantity;
        }

        // Check absolute position against limit
        final long absNewPosition = newPosition >= 0 ? newPosition : -newPosition;
        if (absNewPosition > maxPosition) {
            if (log.isDebugEnabled()) {
                log.debug("Position limit exceeded for user {} market {}: current={} order={} max={}",
                        userId, marketId, FixedPoint.format(currentPosition),
                        FixedPoint.format(quantity), FixedPoint.format(maxPosition));
            }
            return RiskResult.reject(RiskRejectReason.POSITION_LIMIT_EXCEEDED);
        }

        return RiskResult.PASS;
    }

    /**
     * Update the tracked position for a user in a market after a fill.
     *
     * @param userId   user
     * @param marketId market
     * @param side     side of the fill
     * @param quantity filled quantity in fixed-point
     */
    public void onFill(long userId, int marketId, OrderSide side, long quantity) {
        long[] positions = userPositions.get(userId);
        if (positions == null) {
            positions = new long[maxMarkets];
            userPositions.put(userId, positions);
        } else if (marketId >= positions.length) {
            // Grow array if needed (unlikely with pre-sized maxMarkets)
            long[] newPositions = new long[maxMarkets];
            System.arraycopy(positions, 0, newPositions, 0, positions.length);
            positions = newPositions;
            userPositions.put(userId, positions);
        }

        if (side == OrderSide.BUY) {
            positions[marketId] += quantity;
        } else {
            positions[marketId] -= quantity;
        }
    }

    /**
     * Get the current tracked position for a user in a market.
     *
     * @return signed position in fixed-point (positive = net long, negative = net short)
     */
    public long getPosition(long userId, int marketId) {
        long[] positions = userPositions.get(userId);
        if (positions == null || marketId >= positions.length) {
            return 0L;
        }
        return positions[marketId];
    }

    /**
     * Reset all rate limit state. Useful for testing or after a maintenance window.
     */
    public void resetRateLimits() {
        rateLimitStates.clear();
    }

    /**
     * Reset all tracked state (rate limits, open order counts, positions).
     */
    public void resetAll() {
        rateLimitStates.clear();
        openOrderCounts.clear();
        userPositions.clear();
        for (int i = 0; i < maxMarkets; i++) {
            circuitBreakerTripped[i] = false;
        }
    }

    private RiskConfig getConfig(int marketId) {
        if (marketId < 0 || marketId >= maxMarkets) {
            return null;
        }
        return marketConfigs[marketId];
    }
}
