package com.openexchange.oms.common.domain;

/**
 * Risk rejection reason codes.
 */
public final class RiskRejectReason {
    private RiskRejectReason() {}

    public static final String RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED";
    public static final String CIRCUIT_BREAKER_OPEN = "CIRCUIT_BREAKER_OPEN";
    public static final String ORDER_SIZE_TOO_SMALL = "ORDER_SIZE_TOO_SMALL";
    public static final String ORDER_SIZE_TOO_LARGE = "ORDER_SIZE_TOO_LARGE";
    public static final String NOTIONAL_TOO_SMALL = "NOTIONAL_TOO_SMALL";
    public static final String NOTIONAL_TOO_LARGE = "NOTIONAL_TOO_LARGE";
    public static final String PRICE_COLLAR_BREACH = "PRICE_COLLAR_BREACH";
    public static final String OPEN_ORDER_LIMIT = "OPEN_ORDER_LIMIT";
    public static final String INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE";
    public static final String POSITION_LIMIT_EXCEEDED = "POSITION_LIMIT_EXCEEDED";
    public static final String HOLD_FAILED = "HOLD_FAILED";
    public static final String CLUSTER_REJECT = "CLUSTER_REJECT";
    public static final String INVALID_ORDER = "INVALID_ORDER";
    public static final String MARKET_HALTED = "MARKET_HALTED";
}
