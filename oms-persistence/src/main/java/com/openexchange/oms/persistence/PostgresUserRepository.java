// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * PostgreSQL-backed store for demo user accounts (OMS_AUTH_MODE=demo).
 * userIds come from the demo_user_seq sequence (100000-899999, see
 * V002__users.sql). One active bearer token per user, rotated on login.
 */
public class PostgresUserRepository {

    /** A stored user account. Money never lives here — only identity. */
    public record UserRecord(long userId, String username, String passwordHash, String token) {}

    private static final String INSERT_USER = """
            INSERT INTO users (username, password_hash, token)
            VALUES (?, ?, ?)
            ON CONFLICT (username) DO NOTHING
            RETURNING user_id
            """;

    private static final String SELECT_BY_USERNAME = """
            SELECT user_id, username, password_hash, token FROM users WHERE username = ?
            """;

    private static final String SELECT_BY_TOKEN = """
            SELECT user_id, username, password_hash, token FROM users WHERE token = ?
            """;

    private static final String UPDATE_TOKEN = """
            UPDATE users SET token = ? WHERE user_id = ?
            """;

    private final HikariDataSource dataSource;

    public PostgresUserRepository(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Create a user with an initial token.
     *
     * @return the allocated userId, or -1 when the username is already taken
     */
    public long createUser(String username, String passwordHash, String token) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_USER)) {
            ps.setString(1, username);
            ps.setString(2, passwordHash);
            ps.setString(3, token);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : -1;
            }
        } catch (SQLException e) {
            throw new PersistenceException("Failed to create user", e);
        }
    }

    public UserRecord findByUsername(String username) {
        return findBy(SELECT_BY_USERNAME, username);
    }

    public UserRecord findByToken(String token) {
        return findBy(SELECT_BY_TOKEN, token);
    }

    /** Rotate the user's active token (single-session semantics). */
    public void updateToken(long userId, String token) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPDATE_TOKEN)) {
            ps.setString(1, token);
            ps.setLong(2, userId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new PersistenceException("Failed to update token", e);
        }
    }

    private UserRecord findBy(String sql, String value) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, value);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new UserRecord(rs.getLong("user_id"), rs.getString("username"),
                        rs.getString("password_hash"), rs.getString("token"));
            }
        } catch (SQLException e) {
            throw new PersistenceException("Failed to load user", e);
        }
    }
}
