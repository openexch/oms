// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.openexchange.oms.risk.RiskConfigManager;
import com.openexchange.oms.risk.RiskConfigStore;
import com.openexchange.oms.risk.RiskEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Boot-time replay of persisted risk config over the hardcoded defaults. Each stored map
 * goes through {@link RiskConfigManager#updateConfig} (the same merge path the admin API
 * uses), so stored fields override defaults and markets without a row keep them. Rows with
 * {@code manual_trip} re-arm the circuit breaker; automatic trips are never persisted, so
 * they never re-arm.
 */
final class RiskConfigBootstrap {

    private static final Logger log = LoggerFactory.getLogger(RiskConfigBootstrap.class);

    record Result(int marketsLoaded, int tripsRearmed) {
    }

    private RiskConfigBootstrap() {
    }

    static Result replay(Map<Integer, RiskConfigStore.StoredRow> rows,
                         RiskConfigManager configManager, RiskEngine riskEngine) {
        int markets = 0;
        int trips = 0;
        for (Map.Entry<Integer, RiskConfigStore.StoredRow> e : rows.entrySet()) {
            final int marketId = e.getKey();
            try {
                configManager.updateConfig(marketId, e.getValue().config());
                markets++;
            } catch (RuntimeException ex) {
                // A corrupt or out-of-range row must not take boot down; that market
                // simply runs on defaults until the next admin update.
                log.warn("Skipping stored risk config for market {}: {}", marketId, ex.toString());
            }
            // Re-arm independently of the config merge: a corrupt config document must
            // not silently drop an operator's halt on the market.
            if (e.getValue().manualTrip()) {
                try {
                    riskEngine.tripCircuitBreakerManual(marketId);
                    trips++;
                } catch (RuntimeException ex) {
                    log.warn("Cannot re-arm manual circuit-breaker trip for market {}: {}",
                            marketId, ex.toString());
                }
            }
        }
        return new Result(markets, trips);
    }
}
