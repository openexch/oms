package com.openexchange.oms.common.enums;

/**
 * Extended order types supported by OMS.
 * The matching engine only supports LIMIT, MARKET, LIMIT_MAKER.
 * Synthetic types (STOP_*) are managed by the OMS SyntheticOrderEngine.
 */
public enum OmsOrderType {
    /** Standard limit order */
    LIMIT,
    /** Market order — executes at best available price */
    MARKET,
    /** Post-only limit order — rejected if it would match immediately */
    LIMIT_MAKER,
    /** Stop-loss: triggers MARKET order when stop price reached */
    STOP_LOSS,
    /** Stop-limit: triggers LIMIT order when stop price reached */
    STOP_LIMIT,
    /** Trailing stop: tracks price extreme, triggers when price reverses by delta */
    TRAILING_STOP,
    /** Iceberg: large order executed in small visible slices */
    ICEBERG;

    /**
     * Whether this order type is synthetic (managed by OMS, not sent directly to engine).
     */
    public boolean isSynthetic() {
        return this == STOP_LOSS || this == STOP_LIMIT || this == TRAILING_STOP || this == ICEBERG;
    }

    /**
     * Whether this is a basic type supported directly by the matching engine.
     */
    public boolean isEngineNative() {
        return this == LIMIT || this == MARKET || this == LIMIT_MAKER;
    }
}
