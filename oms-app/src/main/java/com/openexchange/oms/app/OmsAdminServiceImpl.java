package com.openexchange.oms.app;

import com.openexchange.oms.api.AdminService;
import com.openexchange.oms.risk.RiskConfigManager;
import com.openexchange.oms.risk.RiskEngine;

import java.util.Map;

/**
 * Admin service implementation delegating to risk engine and config manager.
 */
public class OmsAdminServiceImpl implements AdminService {

    private final RiskConfigManager configManager;
    private final RiskEngine riskEngine;

    public OmsAdminServiceImpl(RiskConfigManager configManager, RiskEngine riskEngine) {
        this.configManager = configManager;
        this.riskEngine = riskEngine;
    }

    @Override
    public Map<String, Object> getRiskConfig(int marketId) {
        return configManager.getConfigAsMap(marketId);
    }

    @Override
    public Map<String, Map<String, Object>> getAllRiskConfigs() {
        return configManager.getAllConfigs();
    }

    @Override
    public void updateRiskConfig(int marketId, Map<String, Object> configFields) {
        configManager.updateConfig(marketId, configFields);
    }

    @Override
    public void tripCircuitBreaker(int marketId) {
        riskEngine.tripCircuitBreaker(marketId);
    }

    @Override
    public void resetCircuitBreaker(int marketId) {
        riskEngine.resetCircuitBreaker(marketId);
    }
}
