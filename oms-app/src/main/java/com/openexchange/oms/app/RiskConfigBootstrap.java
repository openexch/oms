// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.openexchange.oms.risk.RiskConfigManager;
import com.openexchange.oms.risk.RiskConfigStore;
import com.openexchange.oms.risk.RiskEngine;

import java.util.Map;

/**
 * Boot-time replay of persisted risk config over the hardcoded defaults. Each stored map
 * goes through {@link RiskConfigManager#updateConfig} (the same merge path the admin API
 * uses), so stored fields override defaults and markets without a row keep them. Rows with
 * {@code manual_trip} re-arm the circuit breaker; automatic trips are never persisted, so
 * they never re-arm.
 */
final class RiskConfigBootstrap {


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
                throw new IllegalStateException("Cannot restore risk config for market " + marketId, ex);
            }
            // Restore the halt before admission. Any failure aborts the rebuild.
            if (e.getValue().manualTrip()) {
                try {
                    riskEngine.tripCircuitBreakerManual(marketId);
                    trips++;
                } catch (RuntimeException ex) {
                    throw new IllegalStateException("Cannot restore market halt for market " + marketId, ex);
                }
            }
        }
        return new Result(markets, trips);
    }
}
