// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.openexchange.oms.api.OrderService;
import com.openexchange.oms.api.auth.AuthService;
import com.openexchange.oms.api.auth.AuthenticationException;
import com.openexchange.oms.api.auth.PasswordHasher;
import com.openexchange.oms.api.auth.Principal;
import com.openexchange.oms.persistence.PostgresUserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * Demo account service (OMS_AUTH_MODE=demo): Postgres-backed users
 * (V002__users.sql), PBKDF2 password hashing, one active opaque bearer token
 * per user rotated on login, and demo balances funded at registration so a
 * new user can trade immediately.
 */
public final class DemoAuthService implements AuthService {

    private static final Logger log = LoggerFactory.getLogger(DemoAuthService.class);

    private static final Pattern USERNAME = Pattern.compile("[a-z0-9_]{3,20}");
    private static final int MIN_PASSWORD_LENGTH = 6;

    /** 8-dp fixed point, matching the money convention across the stack. */
    private static final long FP = 100_000_000L;

    /**
     * Demo grant per asset at registration: enough USD to buy, plus starting
     * inventory on every listed asset so both order sides work immediately.
     * {assetId, fixed-point amount}: USD 100k, BTC 1, ETH 10, SOL 100,
     * XRP 10k, DOGE 100k.
     */
    private static final long[][] DEMO_GRANTS = {
            {0, 100_000 * FP},
            {1, 1 * FP},
            {2, 10 * FP},
            {3, 100 * FP},
            {4, 10_000 * FP},
            {5, 100_000 * FP},
    };

    private final PostgresUserRepository users;
    private final OrderService orderService;

    public DemoAuthService(PostgresUserRepository users, OrderService orderService) {
        this.users = users;
        this.orderService = orderService;
    }

    @Override
    public Session register(String username, String password) {
        requirePersistence();
        if (username == null || !USERNAME.matcher(username).matches()) {
            throw new IllegalArgumentException(
                    "Username must be 3-20 characters: lowercase letters, digits, underscore");
        }
        if (password == null || password.length() < MIN_PASSWORD_LENGTH) {
            throw new IllegalArgumentException(
                    "Password must be at least " + MIN_PASSWORD_LENGTH + " characters");
        }
        String token = PasswordHasher.newToken();
        long userId = users.createUser(username, PasswordHasher.hash(password), token);
        if (userId < 0) {
            throw new UsernameTakenException(username);
        }
        fundDemoBalances(userId);
        return new Session(userId, username, token);
    }

    @Override
    public Session login(String username, String password) {
        requirePersistence();
        if (username == null || password == null) {
            return null;
        }
        PostgresUserRepository.UserRecord user = users.findByUsername(username);
        if (user == null || !PasswordHasher.verify(password, user.passwordHash())) {
            return null;
        }
        String token = PasswordHasher.newToken();
        users.updateToken(user.userId(), token);
        return new Session(user.userId(), user.username(), token);
    }

    @Override
    public Principal authenticateToken(String token) throws AuthenticationException {
        if (users == null) {
            throw new AuthenticationException("User accounts unavailable: OMS is running without persistence");
        }
        PostgresUserRepository.UserRecord user;
        try {
            user = users.findByToken(token);
        } catch (Exception e) {
            throw new AuthenticationException("User store unavailable");
        }
        if (user == null) {
            throw new AuthenticationException("Invalid or expired token");
        }
        // Self-scoped: no ANY_USER, no ADMIN. Every canActAs check confines
        // this principal to its own orders/balances/history/WS channel.
        return new Principal("user-" + user.username(), user.userId(), Set.of());
    }

    private void requirePersistence() {
        if (users == null) {
            throw new IllegalStateException("User accounts unavailable: OMS is running without persistence");
        }
    }

    /** Best-effort demo funding; a failed asset grant must not fail registration. */
    private void fundDemoBalances(long userId) {
        for (long[] grant : DEMO_GRANTS) {
            try {
                orderService.deposit(userId, (int) grant[0], grant[1]);
            } catch (Exception e) {
                log.warn("Demo funding failed for user {} asset {}: {}", userId, grant[0], e.getMessage());
            }
        }
    }
}
