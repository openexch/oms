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
            log.error("Failed to save execution tradeId={} omsOrderId={}",
                    execution.getTradeId(), execution.getOmsOrderId(), e);
            throw new PersistenceException("Failed to save execution", e);
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
