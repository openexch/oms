// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.risk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The manager's merge path is what boot replays persisted rows through: a stored
 * map must override defaults field-by-field, and markets without a stored row
 * must keep the defaults untouched.
 */
class RiskConfigManagerTest {

    private RiskEngine riskEngine;
    private RiskConfigManager manager;
    private RiskConfig defaultConfig;

    @BeforeEach
    void setUp() {
        riskEngine = new RiskEngine(6, new NoopMarketData(), (userId, assetId, amount) -> true);
        manager = new RiskConfigManager(riskEngine, 6);

        defaultConfig = RiskConfig.builder()
                .minQuantity(1L)
                .maxQuantity(Long.MAX_VALUE)
                .minNotional(0L)
                .maxNotional(Long.MAX_VALUE)
                .priceCollarPercent(20)
                .circuitBreakerPercent(10)
                .circuitBreakerWindowMs(60_000L)
                .maxOrdersPerSec(100)
                .maxOrdersPerMin(1000)
                .maxOpenOrders(500)
                .maxPositionPerMarket(Long.MAX_VALUE)
                .build();
        for (int i = 0; i < 6; i++) {
            manager.setConfig(i, defaultConfig);
        }
    }

    @Test
    void storedMapOverridesDefaults_unstoredFieldsAndMarketsKeepThem() {
        // A partial stored row (as JSON deserialization delivers it: Integer values)
        manager.updateConfig(1, Map.of("maxOpenOrders", 42, "minQuantity", 5));

        RiskConfig updated = manager.getConfig(1);
        assertEquals(42, updated.getMaxOpenOrders());
        assertEquals(5L, updated.getMinQuantity());
        // Fields absent from the stored map keep the default values
        assertEquals(defaultConfig.getMaxQuantity(), updated.getMaxQuantity());
        assertEquals(defaultConfig.getPriceCollarPercent(), updated.getPriceCollarPercent());
        assertEquals(defaultConfig.getCircuitBreakerWindowMs(), updated.getCircuitBreakerWindowMs());
        // Markets without a stored row are untouched
        assertSame(defaultConfig, manager.getConfig(0));
        assertSame(defaultConfig, manager.getConfig(5));
    }

    @Test
    void fullMapReplayRestoresEveryField() {
        manager.updateConfig(2, Map.of(
                "minQuantity", 7L,
                "maxQuantity", 900L,
                "minNotional", 3L,
                "maxNotional", 12_000L,
                "priceCollarPercent", 15,
                "circuitBreakerPercent", 8,
                "circuitBreakerWindowMs", 30_000L,
                "maxOrdersPerSec", 9,
                "maxOrdersPerMin", 99));
        Map<String, Object> full = manager.getConfigAsMap(2);

        // A fresh process: defaults applied first, then the stored full map replays
        RiskConfigManager fresh = new RiskConfigManager(
                new RiskEngine(6, new NoopMarketData(), (userId, assetId, amount) -> true), 6);
        fresh.setConfig(2, defaultConfig);
        fresh.updateConfig(2, full);

        assertEquals(full, fresh.getConfigAsMap(2));
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
