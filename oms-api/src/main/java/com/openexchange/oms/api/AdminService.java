// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api;

import java.util.Map;

/**
 * Admin API service interface for risk configuration management and circuit breaker control.
 *
 * <p>Apply and persist are split on purpose: the in-memory apply stays on the caller's
 * thread (cheap, as before), while the persist methods do JDBC and must be called off the
 * Netty event loop. A failed persist never rolls back the in-memory apply; the endpoint
 * reports durability via {@code "persisted"} in the response.</p>
 */
public interface AdminService {

    Map<String, Object> getRiskConfig(int marketId);

    Map<String, Map<String, Object>> getAllRiskConfigs();

    void updateRiskConfig(int marketId, Map<String, Object> configFields);

    void tripCircuitBreaker(int marketId);

    void resetCircuitBreaker(int marketId);

    /**
     * Persist the current FULL effective config map for a market.
     *
     * @return true when durably stored; false when persistence is not configured or the write failed
     */
    boolean persistRiskConfig(int marketId);

    /**
     * Persist the manual circuit-breaker state for a market (trip = true, reset = false).
     *
     * @return true when durably stored; false when persistence is not configured or the write failed
     */
    boolean persistManualTrip(int marketId, boolean tripped);
}
