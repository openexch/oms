// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.auth;

import java.util.Set;

/**
 * Public-demo provider (OMS_AUTH_MODE=demo): registered users authenticate
 * with opaque bearer tokens minted by {@link AuthService}; their principals
 * are scoped to their own userId (no ANY_USER), so every existing canActAs
 * check confines them to their own orders/balances/history.
 *
 * A narrow dev-token backdoor remains for local infrastructure: the market
 * simulator and ops tooling send "dev:&lt;userId&gt;" per acting user. It is
 * restricted to userId 1 (ops; keeps ADMIN for the risk-config endpoints) and
 * the sim bot/canary range 900000-900999 — and, crucially, the principal is
 * SELF-scoped (no ANY_USER), so a dev token can never read or act on a
 * registered user's data. Registered-range ids (100000-899999) are rejected
 * outright.
 */
public final class DemoAuthenticationProvider implements AuthenticationProvider {

    static final long OPS_USER_ID = 1;
    static final long SIM_RANGE_START = 900_000;
    static final long SIM_RANGE_END = 900_999;

    private final AuthService authService; // null when running without persistence

    public DemoAuthenticationProvider(AuthService authService) {
        this.authService = authService;
    }

    @Override
    public Principal authenticate(Headers headers) throws AuthenticationException {
        String token = AuthenticationProvider.extractToken(headers);
        if (token == null || token.isEmpty()) {
            throw new AuthenticationException("Missing bearer token");
        }
        if (token.startsWith("dev:")) {
            return devPrincipal(token);
        }
        if (authService == null) {
            throw new AuthenticationException("User accounts unavailable: OMS is running without persistence");
        }
        return authService.authenticateToken(token);
    }

    private static Principal devPrincipal(String token) throws AuthenticationException {
        long userId;
        try {
            userId = Long.parseLong(token.substring(4));
        } catch (NumberFormatException e) {
            throw new AuthenticationException("Invalid dev token");
        }
        if (userId == OPS_USER_ID) {
            return new Principal("dev-ops", userId, Set.of(Principal.ROLE_ADMIN));
        }
        if (userId >= SIM_RANGE_START && userId <= SIM_RANGE_END) {
            return new Principal("dev-sim-" + userId, userId, Set.of());
        }
        throw new AuthenticationException("dev tokens are not valid for this user id");
    }
}
