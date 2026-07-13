// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.openexchange.oms.risk.MarketDataProvider;
import com.openexchange.oms.risk.RiskConfig;
import com.openexchange.oms.risk.RiskConfigManager;
import com.openexchange.oms.risk.RiskConfigStore;
import com.openexchange.oms.risk.RiskEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Persist semantics of the admin service: a store failure returns false (the endpoint
 * then answers 200 with persisted:false), bumps the failure counter, and never rolls
 * back the in-memory apply; a missing store (PG not configured) is not a failure.
 */
class OmsAdminServiceImplTest {

    private RiskEngine riskEngine;
    private RiskConfigManager configManager;

    @BeforeEach
    void setUp() {
        riskEngine = new RiskEngine(6, new NoopMarketData(), (userId, assetId, amount) -> true);
        configManager = new RiskConfigManager(riskEngine, 6);
        configManager.setConfig(1, RiskConfig.builder()
                .minQuantity(1L)
                .maxOpenOrders(500)
                .build());
    }

    @Test
    void persistRiskConfig_success_savesFullEffectiveMap() {
        RecordingStore store = new RecordingStore();
        OmsAdminServiceImpl admin = new OmsAdminServiceImpl(configManager, riskEngine, store);

        admin.updateRiskConfig(1, Map.of("maxOpenOrders", 42));
        assertTrue(admin.persistRiskConfig(1));

        assertEquals(1, store.savedMarketId);
        assertEquals(configManager.getConfigAsMap(1), store.savedConfig);
        assertEquals(42, store.savedConfig.get("maxOpenOrders"));
        assertEquals(0, admin.getPersistFailures());
    }

    @Test
    void persistRiskConfig_storeThrows_returnsFalseKeepsMemoryApplyCountsFailure() {
        OmsAdminServiceImpl admin = new OmsAdminServiceImpl(configManager, riskEngine, new ThrowingStore());

        admin.updateRiskConfig(1, Map.of("maxOpenOrders", 42));
        assertFalse(admin.persistRiskConfig(1));

        // In-memory apply survives the failed persist
        assertEquals(42, configManager.getConfig(1).getMaxOpenOrders());
        assertEquals(1, admin.getPersistFailures());
    }

    @Test
    void persistWithoutStore_returnsFalseWithoutCountingFailure() {
        OmsAdminServiceImpl admin = new OmsAdminServiceImpl(configManager, riskEngine, null);
        assertFalse(admin.persistRiskConfig(1));
        assertFalse(admin.persistManualTrip(1, true));
        assertEquals(0, admin.getPersistFailures());
    }

    @Test
    void persistRiskConfig_unconfiguredMarket_returnsFalse() {
        OmsAdminServiceImpl admin = new OmsAdminServiceImpl(configManager, riskEngine, new RecordingStore());
        assertFalse(admin.persistRiskConfig(5)); // no config set for market 5
    }

    @Test
    void tripViaAdminEndpointIsManual_persistManualTripRoundTrips() {
        RecordingStore store = new RecordingStore();
        OmsAdminServiceImpl admin = new OmsAdminServiceImpl(configManager, riskEngine, store);

        admin.tripCircuitBreaker(1);
        assertTrue(riskEngine.isCircuitBreakerTripped(1));
        assertTrue(riskEngine.isCircuitBreakerManuallyTripped(1));

        assertTrue(admin.persistManualTrip(1, true));
        assertEquals(Boolean.TRUE, store.savedTrip);

        admin.resetCircuitBreaker(1);
        assertFalse(riskEngine.isCircuitBreakerTripped(1));
        assertTrue(admin.persistManualTrip(1, false));
        assertEquals(Boolean.FALSE, store.savedTrip);
    }

    @Test
    void persistManualTrip_storeThrows_returnsFalseCountsFailure() {
        OmsAdminServiceImpl admin = new OmsAdminServiceImpl(configManager, riskEngine, new ThrowingStore());
        admin.tripCircuitBreaker(1);
        assertFalse(admin.persistManualTrip(1, true));
        assertTrue(riskEngine.isCircuitBreakerTripped(1)); // trip stays applied
        assertEquals(1, admin.getPersistFailures());
    }

    // -- Test stores --

    private static final class RecordingStore implements RiskConfigStore {
        int savedMarketId = -1;
        Map<String, Object> savedConfig;
        Boolean savedTrip;

        @Override
        public void saveConfig(int marketId, Map<String, Object> config) {
            savedMarketId = marketId;
            savedConfig = new HashMap<>(config);
        }

        @Override
        public void saveManualTrip(int marketId, boolean tripped) {
            savedMarketId = marketId;
            savedTrip = tripped;
        }

        @Override
        public Map<Integer, StoredRow> loadAll() {
            return Map.of();
        }
    }

    private static final class ThrowingStore implements RiskConfigStore {
        @Override
        public void saveConfig(int marketId, Map<String, Object> config) throws Exception {
            throw new SQLException("PG down");
        }

        @Override
        public void saveManualTrip(int marketId, boolean tripped) throws Exception {
            throw new SQLException("PG down");
        }

        @Override
        public Map<Integer, StoredRow> loadAll() {
            return Map.of();
        }
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
