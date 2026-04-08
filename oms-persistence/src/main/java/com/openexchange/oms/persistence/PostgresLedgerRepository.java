package com.openexchange.oms.persistence;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL-backed repository for ledger entries.
 * Optimized for batch inserts — ledger entries are typically written in journal groups.
 */
public class PostgresLedgerRepository {

    private static final Logger log = LoggerFactory.getLogger(PostgresLedgerRepository.class);

    private static final String INSERT_ENTRY = """
            INSERT INTO ledger_entries (journal_id, user_id, asset_id, amount, entry_type, is_debit, reference_id, entry_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_BY_USER = """
            SELECT entry_id, journal_id, user_id, asset_id, amount, entry_type, is_debit, reference_id, entry_time
            FROM ledger_entries WHERE user_id = ?
            ORDER BY entry_time DESC LIMIT ? OFFSET ?
            """;

    private final HikariDataSource dataSource;

    public PostgresLedgerRepository(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Batch-inserts ledger entries in a single transaction.
     * All entries in a journal should be written atomically.
     */
    public void saveLedgerEntries(List<LedgerEntryRecord> entries) {
        if (entries.isEmpty()) {
            return;
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_ENTRY)) {

            conn.setAutoCommit(false);
            try {
                for (LedgerEntryRecord entry : entries) {
                    ps.setLong(1, entry.journalId());
                    ps.setLong(2, entry.userId());
                    ps.setInt(3, entry.assetId());
                    ps.setLong(4, entry.amount());
                    ps.setString(5, entry.entryType());
                    ps.setBoolean(6, entry.isDebit());
                    ps.setLong(7, entry.referenceId());
                    ps.setTimestamp(8, Timestamp.from(Instant.ofEpochMilli(entry.entryTimeMs())));
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

        } catch (SQLException e) {
            log.error("Failed to save ledger entries batch size={}", entries.size(), e);
            throw new PersistenceException("Failed to save ledger entries", e);
        }
    }

    /**
     * Returns ledger entries for a user with pagination, ordered by most recent first.
     */
    public List<LedgerEntryRecord> findByUser(long userId, int limit, int offset) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_BY_USER)) {

            ps.setLong(1, userId);
            ps.setInt(2, limit);
            ps.setInt(3, offset);

            List<LedgerEntryRecord> results = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.add(mapRow(rs));
                }
            }
            return results;

        } catch (SQLException e) {
            log.error("Failed to find ledger entries for userId={}", userId, e);
            throw new PersistenceException("Failed to find ledger entries", e);
        }
    }

    private LedgerEntryRecord mapRow(ResultSet rs) throws SQLException {
        return new LedgerEntryRecord(
                rs.getLong("entry_id"),
                rs.getLong("journal_id"),
                rs.getLong("user_id"),
                rs.getInt("asset_id"),
                rs.getLong("amount"),
                rs.getString("entry_type"),
                rs.getBoolean("is_debit"),
                rs.getLong("reference_id"),
                rs.getTimestamp("entry_time").toInstant().toEpochMilli()
        );
    }
}
