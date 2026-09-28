// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.*;
import java.util.Objects;

/** Durable, per-user API request identity; financial actions never run inside its SQL transaction. */
public class PostgresOrderRequestRepository {
    public record Entry(long omsOrderId, String requestHash, Boolean accepted, String status, String rejectReason) {
        public boolean complete() { return accepted != null; }
    }
    public record Claim(boolean created, Entry entry) {}
    private final HikariDataSource dataSource;

    public PostgresOrderRequestRepository(HikariDataSource dataSource) { this.dataSource = dataSource; }

    public Entry find(long userId, String requestId) {
        try (var c = dataSource.getConnection()) { return find(c, userId, requestId); }
        catch (SQLException e) { throw new PersistenceException("Cannot read durable order request", e); }
    }

    public Claim claim(long userId, String requestId, String hash, long proposedOrderId) {
        try (var c = dataSource.getConnection(); var p = c.prepareStatement("""
                INSERT INTO oms_order_requests(user_id, request_id, request_hash, oms_order_id)
                VALUES (?, ?, ?, ?) ON CONFLICT (user_id, request_id) DO NOTHING
                """)) {
            p.setLong(1, userId); p.setString(2, requestId); p.setString(3, hash); p.setLong(4, proposedOrderId);
            boolean created = p.executeUpdate() == 1;
            Entry entry = find(c, userId, requestId);
            if (entry == null) throw new SQLException("Durable request disappeared after claim");
            return new Claim(created, entry);
        } catch (SQLException e) { throw new PersistenceException("Cannot claim durable order request", e); }
    }

    public void complete(long userId, String requestId, Entry result) {
        if (!result.complete()) throw new IllegalArgumentException("A completed response is required");
        try (var c = dataSource.getConnection(); var p = c.prepareStatement("""
                UPDATE oms_order_requests SET oms_order_id=?, accepted=?, response_status=?,
                    reject_reason=?, completed_at=NOW()
                WHERE user_id=? AND request_id=? AND request_hash=? AND accepted IS NULL
                """)) {
            p.setLong(1, result.omsOrderId()); p.setBoolean(2, result.accepted()); p.setString(3, result.status());
            p.setString(4, result.rejectReason()); p.setLong(5, userId); p.setString(6, requestId);
            p.setString(7, result.requestHash());
            if (p.executeUpdate() == 0 && !Objects.equals(result, find(c, userId, requestId))) {
                throw new SQLException("Durable request completion conflict");
            }
        } catch (SQLException e) { throw new PersistenceException("Cannot commit durable order response", e); }
    }

    private Entry find(Connection c, long userId, String requestId) throws SQLException {
        try (var p = c.prepareStatement("""
                SELECT oms_order_id, request_hash, accepted, response_status, reject_reason
                FROM oms_order_requests WHERE user_id=? AND request_id=?
                """)) {
            p.setLong(1, userId); p.setString(2, requestId);
            try (var r = p.executeQuery()) {
                return r.next() ? new Entry(r.getLong(1), r.getString(2), (Boolean)r.getObject(3),
                        r.getString(4), r.getString(5)) : null;
            }
        }
    }
}
