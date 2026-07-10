// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * PostgreSQL-backed {@link BalanceReadModelStore}: batched upsert into the {@code account_balances}
 * mirror (CQRS read model). Reuses the existing V001 {@code account_balances} table verbatim — its
 * shape {@code (user_id, asset_id, available, locked, version, updated_at, PK(user_id, asset_id))}
 * already fits the read model, so no new migration is needed. {@code version} doubles as a
 * per-key write counter and {@code updated_at} tracks mirror freshness.
 *
 * <p><b>Not money-authoritative.</b> Nothing on the money path reads this table; it exists for
 * ops/analytics only.</p>
 */
public class PostgresBalanceReadModelStore implements BalanceReadModelStore {

    private static final String UPSERT = """
            INSERT INTO account_balances (user_id, asset_id, available, locked, version, updated_at)
            VALUES (?, ?, ?, ?, 0, NOW())
            ON CONFLICT (user_id, asset_id) DO UPDATE SET
                available = EXCLUDED.available,
                locked = EXCLUDED.locked,
                version = account_balances.version + 1,
                updated_at = NOW()
            """;

    private final HikariDataSource dataSource;

    public PostgresBalanceReadModelStore(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void upsert(List<BalanceReadModelRow> rows) throws SQLException {
        if (rows.isEmpty()) {
            return;
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPSERT)) {

            conn.setAutoCommit(false);
            try {
                for (BalanceReadModelRow row : rows) {
                    ps.setLong(1, row.userId());
                    ps.setInt(2, row.assetId());
                    ps.setLong(3, row.available());
                    ps.setLong(4, row.locked());
                    ps.addBatch();
                }
                ps.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }
}
