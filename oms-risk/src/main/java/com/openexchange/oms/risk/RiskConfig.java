package com.openexchange.oms.risk;

/**
 * Per-market risk configuration for the pre-trade risk engine.
 * <p>
 * All monetary values (quantities, notionals) are in fixed-point representation
 * (see {@link com.match.domain.FixedPoint}). Percentages are expressed as integers
 * (e.g. 10 = 10%).
 * <p>
 * Instances are intended to be created at startup or config reload and then read
 * concurrently by the risk engine without synchronization (all fields are final).
 */
public final class RiskConfig {

    // -- Order size limits (per market) --
    private final long minQuantity;
    private final long maxQuantity;
    private final long minNotional;
    private final long maxNotional;

    // -- Price collar (per market) --
    /** Maximum percentage deviation from last trade price for limit orders (e.g. 10 = 10%). */
    private final int priceCollarPercent;

    // -- Circuit breaker (per market) --
    /** Trip threshold: price move percentage within window (e.g. 5 = 5%). */
    private final int circuitBreakerPercent;
    /** Observation window in milliseconds for circuit breaker price monitoring. */
    private final long circuitBreakerWindowMs;

    // -- Rate limits (per user tier) --
    /** Maximum new orders per second for a user. */
    private final int maxOrdersPerSec;
    /** Maximum new orders per minute for a user. */
    private final int maxOrdersPerMin;

    // -- Open order / position limits (per user) --
    /** Maximum concurrent open orders for a user. */
    private final int maxOpenOrders;
    /** Maximum position (in fixed-point base units) per user per market. */
    private final long maxPositionPerMarket;

    private RiskConfig(Builder builder) {
        this.minQuantity = builder.minQuantity;
        this.maxQuantity = builder.maxQuantity;
        this.minNotional = builder.minNotional;
        this.maxNotional = builder.maxNotional;
        this.priceCollarPercent = builder.priceCollarPercent;
        this.circuitBreakerPercent = builder.circuitBreakerPercent;
        this.circuitBreakerWindowMs = builder.circuitBreakerWindowMs;
        this.maxOrdersPerSec = builder.maxOrdersPerSec;
        this.maxOrdersPerMin = builder.maxOrdersPerMin;
        this.maxOpenOrders = builder.maxOpenOrders;
        this.maxPositionPerMarket = builder.maxPositionPerMarket;
    }

    // -- Accessors --

    public long getMinQuantity() {
        return minQuantity;
    }

    public long getMaxQuantity() {
        return maxQuantity;
    }

    public long getMinNotional() {
        return minNotional;
    }

    public long getMaxNotional() {
        return maxNotional;
    }

    public int getPriceCollarPercent() {
        return priceCollarPercent;
    }

    public int getCircuitBreakerPercent() {
        return circuitBreakerPercent;
    }

    public long getCircuitBreakerWindowMs() {
        return circuitBreakerWindowMs;
    }

    public int getMaxOrdersPerSec() {
        return maxOrdersPerSec;
    }

    public int getMaxOrdersPerMin() {
        return maxOrdersPerMin;
    }

    public int getMaxOpenOrders() {
        return maxOpenOrders;
    }

    public long getMaxPositionPerMarket() {
        return maxPositionPerMarket;
    }

    // -- Builder --

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private long minQuantity;
        private long maxQuantity = Long.MAX_VALUE;
        private long minNotional;
        private long maxNotional = Long.MAX_VALUE;
        private int priceCollarPercent = 10;
        private int circuitBreakerPercent = 5;
        private long circuitBreakerWindowMs = 60_000L;
        private int maxOrdersPerSec = 100;
        private int maxOrdersPerMin = 1_000;
        private int maxOpenOrders = 500;
        private long maxPositionPerMarket = Long.MAX_VALUE;

        private Builder() {}

        public Builder minQuantity(long val) { this.minQuantity = val; return this; }
        public Builder maxQuantity(long val) { this.maxQuantity = val; return this; }
        public Builder minNotional(long val) { this.minNotional = val; return this; }
        public Builder maxNotional(long val) { this.maxNotional = val; return this; }
        public Builder priceCollarPercent(int val) { this.priceCollarPercent = val; return this; }
        public Builder circuitBreakerPercent(int val) { this.circuitBreakerPercent = val; return this; }
        public Builder circuitBreakerWindowMs(long val) { this.circuitBreakerWindowMs = val; return this; }
        public Builder maxOrdersPerSec(int val) { this.maxOrdersPerSec = val; return this; }
        public Builder maxOrdersPerMin(int val) { this.maxOrdersPerMin = val; return this; }
        public Builder maxOpenOrders(int val) { this.maxOpenOrders = val; return this; }
        public Builder maxPositionPerMarket(long val) { this.maxPositionPerMarket = val; return this; }

        public RiskConfig build() {
            return new RiskConfig(this);
        }
    }
}
