// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.openexchange.oms.risk.MarketDataProvider;
import com.openexchange.oms.risk.RiskConfig;
import com.openexchange.oms.risk.RiskConfigManager;
import com.openexchange.oms.risk.RiskConfigStore;
import com.openexchange.oms.risk.RiskEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Boot replay: stored rows merge over the hardcoded defaults, manual trips re-arm the
 * breaker, and a corrupt or out-of-range row is skipped instead of failing the boot.
 */
class RiskConfigBootstrapTest {

    private RiskEngine riskEngine;
    private RiskConfigManager configManager;
    private RiskConfig defaults;

    @BeforeEach
    void setUp() {
        riskEngine = new RiskEngine(6, new NoopMarketData(), (userId, assetId, amount) -> true);
        configManager = new RiskConfigManager(riskEngine, 6);
        defaults = RiskConfig.builder()
                .minQuantity(1L)
                .priceCollarPercent(20)
                .maxOpenOrders(500)
                .build();
        for (int i = 0; i < 6; i++) {
            configManager.setConfig(i, defaults);
        }
    }

    @Test
    void replayMergesRowsAndRearmsManualTrips() {
        Map<Integer, RiskConfigStore.StoredRow> rows = Map.of(
                1, new RiskConfigStore.StoredRow(Map.of("maxOpenOrders", 42), true),
                2, new RiskConfigStore.StoredRow(Map.of(), false)); // trip-only row shape

        RiskConfigBootstrap.Result result = RiskConfigBootstrap.replay(rows, configManager, riskEngine);

        assertEquals(2, result.marketsLoaded());
        assertEquals(1, result.tripsRearmed());

        assertEquals(42, configManager.getConfig(1).getMaxOpenOrders());
        assertEquals(20, configManager.getConfig(1).getPriceCollarPercent()); // default kept
        assertTrue(riskEngine.isCircuitBreakerTripped(1));
        assertTrue(riskEngine.isCircuitBreakerManuallyTripped(1));

        assertSame(defaults, configManager.getConfig(0)); // unstored market untouched
        assertEquals(defaults.getMaxOpenOrders(), configManager.getConfig(2).getMaxOpenOrders());
        assertFalse(riskEngine.isCircuitBreakerTripped(2));
    }

    @Test
    void emptyTableIsANoOp() {
        RiskConfigBootstrap.Result result = RiskConfigBootstrap.replay(Map.of(), configManager, riskEngine);
        assertEquals(0, result.marketsLoaded());
        assertEquals(0, result.tripsRearmed());
        assertSame(defaults, configManager.getConfig(1));
    }

    @Test
    void corruptOrOutOfRangeRowsAreSkippedNotFatal() {
        Map<Integer, RiskConfigStore.StoredRow> rows = Map.of(
                1, new RiskConfigStore.StoredRow(Map.of("minQuantity", "garbage"), true),
                99, new RiskConfigStore.StoredRow(Map.of("maxOpenOrders", 1), true),
                3, new RiskConfigStore.StoredRow(Map.of("maxOpenOrders", 7), false));

        RiskConfigBootstrap.Result result = RiskConfigBootstrap.replay(rows, configManager, riskEngine);

        assertEquals(1, result.marketsLoaded());
        assertEquals(7, configManager.getConfig(3).getMaxOpenOrders());
        assertSame(defaults, configManager.getConfig(1)); // corrupt row left market 1 on defaults

        // A corrupt config document must NOT drop the operator's halt on the market
        assertEquals(1, result.tripsRearmed());
        assertTrue(riskEngine.isCircuitBreakerTripped(1));
        assertTrue(riskEngine.isCircuitBreakerManuallyTripped(1));
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
