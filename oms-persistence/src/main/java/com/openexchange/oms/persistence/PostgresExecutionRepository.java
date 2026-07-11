// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

import com.openexchange.oms.common.domain.ExecutionReport;
import com.openexchange.oms.common.enums.OrderSide;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL-backed repository for trade executions.
 * Supports single and batch inserts for high-throughput fill persistence.
 */
public class PostgresExecutionRepository {

    private static final Logger log = LoggerFactory.getLogger(PostgresExecutionRepository.class);

    private static final String INSERT_EXECUTION = """
            INSERT INTO executions (trade_id, oms_order_id, user_id, market_id, side, price, quantity, is_maker, executed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_BY_ORDER = """
            SELECT execution_id, trade_id, oms_order_id, user_id, market_id, side, price, quantity, is_maker, executed_at
            FROM executions WHERE oms_order_id = ?
            ORDER BY executed_at ASC
            """;

    private static final String SELECT_BY_USER = """
            SELECT execution_id, trade_id, oms_order_id, user_id, market_id, side, price, quantity, is_maker, executed_at
            FROM executions WHERE user_id = ?
            ORDER BY executed_at DESC LIMIT ? OFFSET ?
            """;

    private static final String AGGREGATE_POSITIONS = """
            SELECT user_id, market_id,
                   SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) AS net_quantity
            FROM executions
            GROUP BY user_id, market_id
            HAVING SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) <> 0
            """;

    private static final String AGGREGATE_POSITIONS_FOR_USER = """
            SELECT user_id, market_id,
                   SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) AS net_quantity
            FROM executions
            WHERE user_id = ?
            GROUP BY user_id, market_id
            HAVING SUM(CASE WHEN side = 'BUY' THEN quantity ELSE -quantity END) <> 0
            """;

    private static final String MAX_TRADE_ID = "SELECT COALESCE(MAX(trade_id), 0) FROM executions";

    private final HikariDataSource dataSource;

    public PostgresExecutionRepository(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Persists a single execution report.
     */
    public void saveExecution(ExecutionReport execution) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_EXECUTION)) {

            bindExecution(ps, execution);
            ps.executeUpdate();

        } catch (SQLException e) {
            // No log here: the FIRST attempt failing is the NORMAL oms#23 FK race (an instant
            // crossing fill reaching PG before the order row's first upsert) and the caller
            // recovers it — during the 2026-07-11 storm this line burned thousands of stack
            // traces per hour for saves that succeeded on retry. The caller logs final failures.
            throw new PersistenceException("Failed to save execution tradeId="
                    + execution.getTradeId() + " omsOrderId=" + execution.getOmsOrderId(), e);
        }
    }

    /**
     * Persists a batch of execution reports in a single transaction.
     * Uses JDBC batch for maximum throughput.
     */
    public void saveBatch(List<ExecutionReport> executions) {
        if (executions.isEmpty()) {
            return;
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_EXECUTION)) {

            conn.setAutoCommit(false);
            try {
                for (ExecutionReport exec : executions) {
                    bindExecution(ps, exec);
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
            log.error("Failed to save execution batch size={}", executions.size(), e);
            throw new PersistenceException("Failed to save execution batch", e);
        }
    }

    /**
     * Returns all executions for a given order, ordered by execution time ascending.
     */
    public List<ExecutionReport> findByOrder(long omsOrderId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_BY_ORDER)) {

            ps.setLong(1, omsOrderId);
            return collectExecutions(ps);

        } catch (SQLException e) {
            log.error("Failed to find executions for omsOrderId={}", omsOrderId, e);
            throw new PersistenceException("Failed to find executions by order", e);
        }
    }

    /**
     * Returns executions for a user with pagination, ordered by most recent first.
     */
    public List<ExecutionReport> findByUser(long userId, int limit, int offset) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_BY_USER)) {

            ps.setLong(1, userId);
            ps.setInt(2, limit);
            ps.setInt(3, offset);
            return collectExecutions(ps);

        } catch (SQLException e) {
            log.error("Failed to find executions for userId={}", userId, e);
            throw new PersistenceException("Failed to find executions by user", e);
        }
    }

    /**
     * Net position per (user, market) replayed from the full executions ledger
     * — the startup state rebuild source for RiskEngine positions (oms#35).
     * Zero net positions are omitted.
     */
    public List<PositionAggregate> aggregatePositions() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(AGGREGATE_POSITIONS);
             ResultSet rs = ps.executeQuery()) {

            List<PositionAggregate> results = new ArrayList<>();
            while (rs.next()) {
                results.add(new PositionAggregate(
                        rs.getLong("user_id"), rs.getInt("market_id"), rs.getLong("net_quantity")));
            }
            return results;

        } catch (SQLException e) {
            log.error("Failed to aggregate positions from executions", e);
            throw new PersistenceException("Failed to aggregate positions", e);
        }
    }

    /**
     * Net position per market for one user, replayed from the executions
     * ledger — the positions history read (oms#40). Zero nets are omitted.
     */
    public List<PositionAggregate> aggregatePositionsForUser(long userId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(AGGREGATE_POSITIONS_FOR_USER)) {

            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                List<PositionAggregate> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(new PositionAggregate(
                            rs.getLong("user_id"), rs.getInt("market_id"), rs.getLong("net_quantity")));
                }
                return results;
            }

        } catch (SQLException e) {
            log.error("Failed to aggregate positions for userId={}", userId, e);
            throw new PersistenceException("Failed to aggregate positions for user", e);
        }
    }

    /**
     * The highest {@code trade_id} ever persisted, or 0 if the executions table is empty.
     * Boot-time init for the AE-backed balance store's settle-side-effect dedupe high-water
     * (E3/E4): the AE settles money asynchronously off the ME journal feed, so the OMS's local
     * side effects (risk onFill, exec persist, applyFill) need their own floor to survive a
     * restart without re-applying a trade whose execution row was already written.
     */
    public long maxTradeId() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(MAX_TRADE_ID);
             ResultSet rs = ps.executeQuery()) {

            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0L;

        } catch (SQLException e) {
            log.error("Failed to read max(trade_id) from executions", e);
            throw new PersistenceException("Failed to read max trade id", e);
        }
    }

    // ---- internal helpers ----

    private void bindExecution(PreparedStatement ps, ExecutionReport exec) throws SQLException {
        ps.setLong(1, exec.getTradeId());
        ps.setLong(2, exec.getOmsOrderId());
        ps.setLong(3, exec.getUserId());
        ps.setInt(4, exec.getMarketId());
        ps.setString(5, exec.getSide().name());
        ps.setLong(6, exec.getPrice());
        ps.setLong(7, exec.getQuantity());
        ps.setBoolean(8, exec.isMaker());
        ps.setTimestamp(9, Timestamp.from(Instant.ofEpochMilli(exec.getExecutedAtMs())));
    }

    private List<ExecutionReport> collectExecutions(PreparedStatement ps) throws SQLException {
        List<ExecutionReport> results = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                results.add(mapRow(rs));
            }
        }
        return results;
    }

    private ExecutionReport mapRow(ResultSet rs) throws SQLException {
        ExecutionReport e = new ExecutionReport();
        e.setExecutionId(rs.getLong("execution_id"));
        e.setTradeId(rs.getLong("trade_id"));
        e.setOmsOrderId(rs.getLong("oms_order_id"));
        e.setUserId(rs.getLong("user_id"));
        e.setMarketId(rs.getInt("market_id"));
        e.setSide(OrderSide.valueOf(rs.getString("side")));
        e.setPrice(rs.getLong("price"));
        e.setQuantity(rs.getLong("quantity"));
        e.setMaker(rs.getBoolean("is_maker"));
        e.setExecutedAtMs(rs.getTimestamp("executed_at").toInstant().toEpochMilli());
        return e;
    }
}
