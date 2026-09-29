// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

import com.openexchange.oms.common.domain.OmsOrder;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;
import java.util.*;

/** Reads projector tables and commits OMS live state with the OMS journal checkpoint. */
public final class PostgresJournalConsumerRepository implements JournalConsumerStore {
    private final HikariDataSource dataSource;
    private final PostgresOrderRepository orders;

    public PostgresJournalConsumerRepository(HikariDataSource dataSource) {
        this.dataSource = dataSource;
        this.orders = new PostgresOrderRepository(dataSource);
    }

    @Override public Optional<Checkpoint> loadCheckpoint() {
        return readCheckpoint("SELECT source_identity,position,last_trade_id FROM oms_journal_consumer_checkpoint WHERE consumer='oms-live'");
    }

    @Override public Optional<ProjectorStatus> projectorStatus() {
        try (Connection c = dataSource.getConnection(); PreparedStatement p = c.prepareStatement("""
                SELECT source_identity,position,last_trade_id,observed_target,
                       COALESCE(NOW()-observed_at < interval '3 seconds', false)
                FROM execution_projector_checkpoint WHERE consumer='executions'""");
             ResultSet rows = p.executeQuery()) {
            if (!rows.next()) return Optional.empty();
            return Optional.of(new ProjectorStatus(new Checkpoint(rows.getString(1), rows.getLong(2), rows.getLong(3)),
                    rows.getLong(4), rows.getBoolean(5)));
        } catch (SQLException e) {
            throw new PersistenceException("Cannot read projector status", e);
        }
    }

    private Optional<Checkpoint> readCheckpoint(String sql) {
        try (Connection c = dataSource.getConnection(); PreparedStatement p = c.prepareStatement(sql);
             ResultSet rows = p.executeQuery()) {
            if (!rows.next()) return Optional.empty();
            return Optional.of(new Checkpoint(rows.getString(1), rows.getLong(2), rows.getLong(3)));
        } catch (SQLException e) {
            throw new PersistenceException("Cannot read journal checkpoint", e);
        }
    }

    @Override public List<Event> eventsAfter(String source, long position, int limit) {
        if (limit < 1 || limit > 1024) throw new IllegalArgumentException("Invalid journal batch bound");
        try (Connection c = dataSource.getConnection(); PreparedStatement p = c.prepareStatement("""
                SELECT position,template_id,payload FROM execution_journal_events
                WHERE source_identity=? AND position>? ORDER BY position LIMIT ?""")) {
            p.setString(1, source); p.setLong(2, position); p.setInt(3, limit);
            List<Event> result = new ArrayList<>();
            try (ResultSet rows = p.executeQuery()) {
                while (rows.next()) result.add(new Event(rows.getLong(1), rows.getInt(2), rows.getBytes(3)));
            }
            return result;
        } catch (SQLException e) {
            throw new PersistenceException("Cannot read journal events", e);
        }
    }

    @Override public Map<PositionKey, Long> loadPositions() {
        try (Connection c = dataSource.getConnection(); PreparedStatement p = c.prepareStatement(
                "SELECT user_id,market_id,net_quantity FROM oms_risk_positions");
             ResultSet rows = p.executeQuery()) {
            Map<PositionKey, Long> result = new HashMap<>();
            while (rows.next()) result.put(new PositionKey(rows.getLong(1), rows.getInt(2)), rows.getLong(3));
            return result;
        } catch (SQLException e) {
            throw new PersistenceException("Cannot read risk positions", e);
        }
    }

    @Override public boolean hasOrders() {
        try (Connection c = dataSource.getConnection(); PreparedStatement p = c.prepareStatement(
                "SELECT EXISTS(SELECT 1 FROM orders)"); ResultSet rows = p.executeQuery()) {
            rows.next();
            return rows.getBoolean(1);
        } catch (SQLException e) {
            throw new PersistenceException("Cannot read orders", e);
        }
    }

    @Override public void commit(Collection<OmsOrder> touched, Map<PositionKey, Long> positionDeltas,
                                 Optional<Checkpoint> expected, Checkpoint next) {
        if (expected.isPresent() && (!expected.get().source().equals(next.source())
                || next.position() <= expected.get().position() || next.lastTradeId() < expected.get().lastTradeId())) {
            throw new IllegalArgumentException("Journal checkpoint must move forward on the same source");
        }
        List<OmsOrder> ordered = new ArrayList<>(touched);
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            try {
                for (OmsOrder order : ordered) {
                    orders.saveInTransaction(c, order, Math.addExact(order.getStateRevision(), 1));
                }
                try (PreparedStatement p = c.prepareStatement("""
                        INSERT INTO oms_risk_positions(user_id,market_id,net_quantity) VALUES(?,?,?)
                        ON CONFLICT(user_id,market_id) DO UPDATE SET net_quantity=oms_risk_positions.net_quantity+EXCLUDED.net_quantity""")) {
                    for (var delta : positionDeltas.entrySet()) {
                        p.setLong(1, delta.getKey().userId()); p.setInt(2, delta.getKey().marketId());
                        p.setLong(3, delta.getValue()); p.addBatch();
                    }
                    p.executeBatch();
                }
                int moved;
                if (expected.isEmpty()) {
                    try (PreparedStatement p = c.prepareStatement("""
                            INSERT INTO oms_journal_consumer_checkpoint(consumer,source_identity,position,last_trade_id)
                            VALUES('oms-live',?,?,?) ON CONFLICT DO NOTHING""")) {
                        p.setString(1, next.source()); p.setLong(2, next.position()); p.setLong(3, next.lastTradeId());
                        moved = p.executeUpdate();
                    }
                } else {
                    try (PreparedStatement p = c.prepareStatement("""
                            UPDATE oms_journal_consumer_checkpoint SET position=?,last_trade_id=?,updated_at=NOW()
                            WHERE consumer='oms-live' AND source_identity=? AND position=? AND last_trade_id=?""")) {
                        p.setLong(1, next.position()); p.setLong(2, next.lastTradeId()); p.setString(3, next.source());
                        p.setLong(4, expected.get().position()); p.setLong(5, expected.get().lastTradeId());
                        moved = p.executeUpdate();
                    }
                }
                if (moved != 1) throw new PersistenceException("Journal checkpoint moved concurrently");
                c.commit();
            } catch (SQLException | RuntimeException e) {
                c.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new PersistenceException("Journal batch commit failed", e);
        }
        for (OmsOrder order : ordered) {
            order.setStateRevision(order.getStateRevision() + 1);
        }
    }
}
