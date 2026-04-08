package com.openexchange.oms.persistence;

import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.common.enums.OmsOrderType;
import com.openexchange.oms.common.enums.OrderSide;
import com.openexchange.oms.common.enums.TimeInForce;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL-backed repository for OMS orders.
 * Uses HikariCP connection pooling and prepared statements for all operations.
 */
public class PostgresOrderRepository {

    private static final Logger log = LoggerFactory.getLogger(PostgresOrderRepository.class);

    private static final String INSERT_ORDER = """
            INSERT INTO orders (oms_order_id, cluster_order_id, client_order_id, user_id, market_id,
                side, order_type, time_in_force, price, quantity, filled_qty, remaining_qty,
                stop_price, trailing_delta, display_quantity, status, reject_reason, hold_amount,
                expires_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String UPDATE_STATUS = """
            UPDATE orders SET status = ?, filled_qty = ?, remaining_qty = ?, updated_at = NOW()
            WHERE oms_order_id = ?
            """;

    private static final String SELECT_BY_ID = """
            SELECT oms_order_id, cluster_order_id, client_order_id, user_id, market_id,
                side, order_type, time_in_force, price, quantity, filled_qty, remaining_qty,
                stop_price, trailing_delta, display_quantity, status, reject_reason, hold_amount,
                expires_at, created_at, updated_at
            FROM orders WHERE oms_order_id = ?
            """;

    private static final String SELECT_BY_USER_STATUS = """
            SELECT oms_order_id, cluster_order_id, client_order_id, user_id, market_id,
                side, order_type, time_in_force, price, quantity, filled_qty, remaining_qty,
                stop_price, trailing_delta, display_quantity, status, reject_reason, hold_amount,
                expires_at, created_at, updated_at
            FROM orders WHERE user_id = ? AND status = ?
            ORDER BY created_at DESC LIMIT ? OFFSET ?
            """;

    private static final String SELECT_OPEN_ORDERS = """
            SELECT oms_order_id, cluster_order_id, client_order_id, user_id, market_id,
                side, order_type, time_in_force, price, quantity, filled_qty, remaining_qty,
                stop_price, trailing_delta, display_quantity, status, reject_reason, hold_amount,
                expires_at, created_at, updated_at
            FROM orders WHERE user_id = ? AND status NOT IN ('FILLED', 'CANCELLED', 'EXPIRED', 'REJECTED')
            ORDER BY created_at DESC
            """;

    private final HikariDataSource dataSource;

    public PostgresOrderRepository(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Persists a new order. Uses INSERT — caller is responsible for ensuring no duplicate omsOrderId.
     */
    public void saveOrder(OmsOrder order) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_ORDER)) {

            bindOrderInsert(ps, order);
            ps.executeUpdate();

        } catch (SQLException e) {
            log.error("Failed to save order omsOrderId={}", order.getOmsOrderId(), e);
            throw new PersistenceException("Failed to save order", e);
        }
    }

    /**
     * Updates order status and fill quantities atomically.
     */
    public void updateOrderStatus(long omsOrderId, OmsOrderStatus status, long filledQty, long remainingQty) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPDATE_STATUS)) {

            ps.setString(1, status.name());
            ps.setLong(2, filledQty);
            ps.setLong(3, remainingQty);
            ps.setLong(4, omsOrderId);

            int rows = ps.executeUpdate();
            if (rows == 0) {
                log.warn("No order found to update: omsOrderId={}", omsOrderId);
            }

        } catch (SQLException e) {
            log.error("Failed to update order status omsOrderId={}", omsOrderId, e);
            throw new PersistenceException("Failed to update order status", e);
        }
    }

    /**
     * Finds a single order by its OMS order ID.
     *
     * @return the order, or null if not found
     */
    public OmsOrder findById(long omsOrderId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_BY_ID)) {

            ps.setLong(1, omsOrderId);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return mapRow(rs);
                }
                return null;
            }

        } catch (SQLException e) {
            log.error("Failed to find order omsOrderId={}", omsOrderId, e);
            throw new PersistenceException("Failed to find order", e);
        }
    }

    /**
     * Returns orders for a user filtered by status, with pagination.
     */
    public List<OmsOrder> findByUserAndStatus(long userId, OmsOrderStatus status, int limit, int offset) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_BY_USER_STATUS)) {

            ps.setLong(1, userId);
            ps.setString(2, status.name());
            ps.setInt(3, limit);
            ps.setInt(4, offset);

            return collectOrders(ps);

        } catch (SQLException e) {
            log.error("Failed to find orders userId={} status={}", userId, status, e);
            throw new PersistenceException("Failed to find orders by user and status", e);
        }
    }

    /**
     * Returns all non-terminal orders for a user.
     */
    public List<OmsOrder> findOpenOrders(long userId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_OPEN_ORDERS)) {

            ps.setLong(1, userId);

            return collectOrders(ps);

        } catch (SQLException e) {
            log.error("Failed to find open orders userId={}", userId, e);
            throw new PersistenceException("Failed to find open orders", e);
        }
    }

    // ---- internal helpers ----

    private void bindOrderInsert(PreparedStatement ps, OmsOrder order) throws SQLException {
        ps.setLong(1, order.getOmsOrderId());
        ps.setLong(2, order.getClusterOrderId());
        if (order.getClientOrderId() != null) {
            ps.setString(3, order.getClientOrderId());
        } else {
            ps.setNull(3, Types.VARCHAR);
        }
        ps.setLong(4, order.getUserId());
        ps.setInt(5, order.getMarketId());
        ps.setString(6, order.getSide().name());
        ps.setString(7, order.getOrderType().name());
        ps.setString(8, order.getTimeInForce().name());
        ps.setLong(9, order.getPrice());
        ps.setLong(10, order.getQuantity());
        ps.setLong(11, order.getFilledQty());
        ps.setLong(12, order.getRemainingQty());
        ps.setLong(13, order.getStopPrice());
        ps.setLong(14, order.getTrailingDelta());
        ps.setLong(15, order.getDisplayQuantity());
        ps.setString(16, order.getStatus().name());
        if (order.getRejectReason() != null) {
            ps.setString(17, order.getRejectReason());
        } else {
            ps.setNull(17, Types.VARCHAR);
        }
        ps.setLong(18, order.getHoldAmount());
        if (order.getExpiresAtMs() > 0) {
            ps.setTimestamp(19, Timestamp.from(Instant.ofEpochMilli(order.getExpiresAtMs())));
        } else {
            ps.setNull(19, Types.TIMESTAMP);
        }
        ps.setTimestamp(20, Timestamp.from(Instant.ofEpochMilli(order.getCreatedAtMs())));
        ps.setTimestamp(21, Timestamp.from(Instant.ofEpochMilli(order.getUpdatedAtMs())));
    }

    private List<OmsOrder> collectOrders(PreparedStatement ps) throws SQLException {
        List<OmsOrder> orders = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                orders.add(mapRow(rs));
            }
        }
        return orders;
    }

    private OmsOrder mapRow(ResultSet rs) throws SQLException {
        OmsOrder o = new OmsOrder();
        o.setOmsOrderId(rs.getLong("oms_order_id"));
        o.setClusterOrderId(rs.getLong("cluster_order_id"));
        o.setClientOrderId(rs.getString("client_order_id"));
        o.setUserId(rs.getLong("user_id"));
        o.setMarketId(rs.getInt("market_id"));
        o.setSide(OrderSide.valueOf(rs.getString("side")));
        o.setOrderType(OmsOrderType.valueOf(rs.getString("order_type")));
        o.setTimeInForce(TimeInForce.valueOf(rs.getString("time_in_force")));
        o.setPrice(rs.getLong("price"));
        o.setQuantity(rs.getLong("quantity"));
        o.setFilledQty(rs.getLong("filled_qty"));
        o.setRemainingQty(rs.getLong("remaining_qty"));
        o.setStopPrice(rs.getLong("stop_price"));
        o.setTrailingDelta(rs.getLong("trailing_delta"));
        o.setDisplayQuantity(rs.getLong("display_quantity"));
        o.setStatus(OmsOrderStatus.valueOf(rs.getString("status")));
        o.setRejectReason(rs.getString("reject_reason"));
        o.setHoldAmount(rs.getLong("hold_amount"));

        Timestamp expiresAt = rs.getTimestamp("expires_at");
        o.setExpiresAtMs(expiresAt != null ? expiresAt.toInstant().toEpochMilli() : 0);

        o.setCreatedAtMs(rs.getTimestamp("created_at").toInstant().toEpochMilli());
        o.setUpdatedAtMs(rs.getTimestamp("updated_at").toInstant().toEpochMilli());
        return o;
    }
}
