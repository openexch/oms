// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

import com.openexchange.oms.risk.BalanceChecker;
import com.openexchange.oms.risk.MarketDataProvider;
import com.openexchange.oms.risk.RiskConfig;
import com.openexchange.oms.risk.RiskConfigManager;
import com.openexchange.oms.risk.RiskEngine;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JSON round-trip for the persisted config document (no PG integration harness in this
 * repo, so the map-to-JSONB mapping is what gets unit-tested): a full effective config
 * map must survive toJson/fromJson and replay through the manager's merge path to the
 * exact same effective config, including Long.MAX_VALUE sentinels.
 */
class PostgresRiskConfigStoreJsonTest {

    private static final BalanceChecker ALWAYS_SUFFICIENT = (userId, assetId, amount) -> true;

    private static RiskConfigManager newManager() {
        return new RiskConfigManager(new RiskEngine(6, new NoopMarketData(), ALWAYS_SUFFICIENT), 6);
    }

    @Test
    void configMapRoundTripsThroughJson() throws Exception {
        RiskConfigManager manager = newManager();
        manager.setConfig(1, RiskConfig.builder()
                .minQuantity(1L)
                .maxQuantity(Long.MAX_VALUE) // sentinel must survive exactly
                .minNotional(0L)
                .maxNotional(9_000_000_000_000_000_000L)
                .priceCollarPercent(20)
                .circuitBreakerPercent(10)
                .circuitBreakerWindowMs(60_000L)
                .maxOrdersPerSec(100)
                .maxOrdersPerMin(1000)
                .maxOpenOrders(500)
                .maxPositionPerMarket(Long.MAX_VALUE)
                .build());
        Map<String, Object> original = manager.getConfigAsMap(1);

        String json = PostgresRiskConfigStore.toJson(original);
        Map<String, Object> restored = PostgresRiskConfigStore.fromJson(json);

        // Replay the restored map like boot does; the effective config must be identical
        RiskConfigManager fresh = newManager();
        fresh.updateConfig(1, restored);
        assertEquals(original, fresh.getConfigAsMap(1));

        RiskConfig config = fresh.getConfig(1);
        assertEquals(Long.MAX_VALUE, config.getMaxQuantity());
        assertEquals(9_000_000_000_000_000_000L, config.getMaxNotional());
        assertEquals(500, config.getMaxOpenOrders());
    }

    @Test
    void emptyDocumentReplaysAsNoOpMerge() throws Exception {
        // saveManualTrip inserts '{}' for markets with no stored config; replaying it
        // must leave the defaults untouched
        Map<String, Object> restored = PostgresRiskConfigStore.fromJson("{}");

        RiskConfigManager manager = newManager();
        RiskConfig defaults = RiskConfig.builder().minQuantity(1L).maxOpenOrders(500).build();
        manager.setConfig(1, defaults);
        Map<String, Object> before = manager.getConfigAsMap(1);

        manager.updateConfig(1, restored);
        assertEquals(before, manager.getConfigAsMap(1));
    }

    private static final class NoopMarketData implements MarketDataProvider {
        @Override
        public long getLastTradePrice(int marketId) { return 0L; }
        @Override
        public long getBestBid(int marketId) { return 0L; }
        @Override
        public long getBestAsk(int marketId) { return 0L; }
    }
}
