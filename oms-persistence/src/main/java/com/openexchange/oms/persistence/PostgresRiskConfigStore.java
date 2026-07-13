// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openexchange.oms.risk.RiskConfigStore;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * PostgreSQL-backed {@link RiskConfigStore}: upserts into the V003 {@code risk_config} table.
 * The full effective config map is stored as one JSONB document, so schema evolution of
 * {@link com.openexchange.oms.risk.RiskConfig} needs no migration; unknown keys are ignored
 * by the manager's merge path on replay.
 *
 * <p>{@code saveConfig} and {@code saveManualTrip} deliberately touch only their own column
 * so a breaker trip never clobbers a stored config and vice versa. A trip on a market with
 * no stored config inserts an empty {@code {}} document, which replays as a no-op merge.</p>
 */
public class PostgresRiskConfigStore implements RiskConfigStore {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private static final String UPSERT_CONFIG = """
            INSERT INTO risk_config (market_id, config, updated_at)
            VALUES (?, ?::jsonb, NOW())
            ON CONFLICT (market_id) DO UPDATE SET
                config = EXCLUDED.config,
                updated_at = NOW()
            """;

    private static final String UPSERT_TRIP = """
            INSERT INTO risk_config (market_id, config, manual_trip, updated_at)
            VALUES (?, '{}'::jsonb, ?, NOW())
            ON CONFLICT (market_id) DO UPDATE SET
                manual_trip = EXCLUDED.manual_trip,
                updated_at = NOW()
            """;

    private static final String SELECT_ALL =
            "SELECT market_id, config::text, manual_trip FROM risk_config";

    private final HikariDataSource dataSource;

    public PostgresRiskConfigStore(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void saveConfig(int marketId, Map<String, Object> config) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPSERT_CONFIG)) {
            ps.setInt(1, marketId);
            ps.setString(2, toJson(config));
            ps.executeUpdate();
        }
    }

    @Override
    public void saveManualTrip(int marketId, boolean tripped) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPSERT_TRIP)) {
            ps.setInt(1, marketId);
            ps.setBoolean(2, tripped);
            ps.executeUpdate();
        }
    }

    @Override
    public Map<Integer, StoredRow> loadAll() throws Exception {
        Map<Integer, StoredRow> rows = new LinkedHashMap<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_ALL);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                rows.put(rs.getInt(1), new StoredRow(fromJson(rs.getString(2)), rs.getBoolean(3)));
            }
        }
        return rows;
    }

    // Package-private for the JSON round-trip test (no PG integration harness in this repo).

    static String toJson(Map<String, Object> config) throws Exception {
        return MAPPER.writeValueAsString(config);
    }

    static Map<String, Object> fromJson(String json) throws Exception {
        return MAPPER.readValue(json, MAP_TYPE);
    }
}
