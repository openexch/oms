// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.risk;

import java.util.Map;

/**
 * Durability seam for per-market risk config and manual circuit-breaker trips
 * (risk_config table). Plain-JDK types only, so the JDBC implementation lives
 * in {@code oms-persistence} and this module stays persistence-free.
 *
 * <p>Not on the order hot path: writes happen on admin updates, reads once at
 * boot. Implementations may block.</p>
 */
public interface RiskConfigStore {

    /** One stored row: the full effective config map plus the manual-trip flag. */
    record StoredRow(Map<String, Object> config, boolean manualTrip) {
    }

    /**
     * Upsert the FULL effective config map for a market. Must not touch the
     * row's manual_trip flag.
     *
     * @throws Exception if the write fails; the in-memory config stays applied
     */
    void saveConfig(int marketId, Map<String, Object> config) throws Exception;

    /**
     * Upsert the manual circuit-breaker state for a market. Must not touch the
     * row's config document.
     *
     * @throws Exception if the write fails; the in-memory breaker state stays applied
     */
    void saveManualTrip(int marketId, boolean tripped) throws Exception;

    /**
     * Load every stored row keyed by marketId. An empty table yields an empty
     * map, never an error.
     */
    Map<Integer, StoredRow> loadAll() throws Exception;
}
