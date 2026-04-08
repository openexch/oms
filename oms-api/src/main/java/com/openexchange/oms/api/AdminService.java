package com.openexchange.oms.api;

import java.util.Map;

/**
 * Admin API service interface for risk configuration management and circuit breaker control.
 */
public interface AdminService {

    Map<String, Object> getRiskConfig(int marketId);

    Map<String, Map<String, Object>> getAllRiskConfigs();

    void updateRiskConfig(int marketId, Map<String, Object> configFields);

    void tripCircuitBreaker(int marketId);

    void resetCircuitBreaker(int marketId);
}
