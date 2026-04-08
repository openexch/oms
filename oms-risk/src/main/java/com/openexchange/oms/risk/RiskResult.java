package com.openexchange.oms.risk;

/**
 * Immutable result of a pre-trade risk check.
 * <p>
 * The singleton {@link #PASS} instance is reused for all passing checks to avoid allocation.
 * Rejection results are created via {@link #reject(String)} — these allocations are acceptable
 * since rejections are infrequent and trigger logging/response paths that already allocate.
 */
public final class RiskResult {

    /** Pre-allocated singleton for passing checks — zero allocation on the hot path. */
    public static final RiskResult PASS = new RiskResult(true, null);

    private final boolean passed;
    private final String rejectReason;

    private RiskResult(boolean passed, String rejectReason) {
        this.passed = passed;
        this.rejectReason = rejectReason;
    }

    /**
     * Create a rejection result with the given reason.
     *
     * @param reason one of the {@link com.openexchange.oms.common.domain.RiskRejectReason} constants
     * @return a new RiskResult indicating rejection
     */
    public static RiskResult reject(String reason) {
        return new RiskResult(false, reason);
    }

    public boolean isPassed() {
        return passed;
    }

    public String getRejectReason() {
        return rejectReason;
    }

    @Override
    public String toString() {
        return passed ? "RiskResult{PASS}" : "RiskResult{REJECT=" + rejectReason + '}';
    }
}
