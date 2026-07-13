// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.openexchange.oms.api.AdminService;
import com.openexchange.oms.risk.RiskConfigManager;
import com.openexchange.oms.risk.RiskConfigStore;
import com.openexchange.oms.risk.RiskEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Admin service implementation delegating to risk engine and config manager.
 *
 * <p>The persist methods write through the {@link RiskConfigStore} (null when PG is not
 * configured; boot logged the not-durable WARN once). A persist failure never rolls back
 * the in-memory apply: it returns false, WARNs, and bumps the failure counter exported as
 * {@code oms_risk_config_persist_failures_total}. Callers must invoke the persist methods
 * off the Netty event loop (see RestApiHandler's oms-risk-persist executor).</p>
 */
public class OmsAdminServiceImpl implements AdminService {

    private static final Logger log = LoggerFactory.getLogger(OmsAdminServiceImpl.class);

    private final RiskConfigManager configManager;
    private final RiskEngine riskEngine;
    private final RiskConfigStore configStore; // null = memory-only risk config
    private final AtomicLong persistFailures = new AtomicLong();

    public OmsAdminServiceImpl(RiskConfigManager configManager, RiskEngine riskEngine,
                               RiskConfigStore configStore) {
        this.configManager = configManager;
        this.riskEngine = riskEngine;
        this.configStore = configStore;
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
        // The admin endpoint is by definition an operator action: manual, durable.
        riskEngine.tripCircuitBreakerManual(marketId);
    }

    @Override
    public void resetCircuitBreaker(int marketId) {
        riskEngine.resetCircuitBreaker(marketId);
    }

    @Override
    public boolean persistRiskConfig(int marketId) {
        if (configStore == null) {
            return false;
        }
        Map<String, Object> config = configManager.getConfigAsMap(marketId);
        if (config == null) {
            return false;
        }
        try {
            configStore.saveConfig(marketId, config);
            return true;
        } catch (Exception e) {
            persistFailures.incrementAndGet();
            log.warn("Risk config persist failed for market {} (applied in memory, NOT durable): {}",
                    marketId, e.toString());
            return false;
        }
    }

    @Override
    public boolean persistManualTrip(int marketId, boolean tripped) {
        if (configStore == null) {
            return false;
        }
        try {
            configStore.saveManualTrip(marketId, tripped);
            return true;
        } catch (Exception e) {
            persistFailures.incrementAndGet();
            log.warn("Circuit-breaker persist failed for market {} tripped={} (applied in memory, "
                    + "NOT durable): {}", marketId, tripped, e.toString());
            return false;
        }
    }

    /** oms_risk_config_persist_failures_total */
    public long getPersistFailures() {
        return persistFailures.get();
    }
}
