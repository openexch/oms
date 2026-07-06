// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.auth;

/**
 * Simple demo account service (OMS_AUTH_MODE=demo): username/password
 * registration and login minting one active opaque bearer token per user.
 * Implemented in oms-app (needs the Postgres user store and the funding
 * path); consumed by RestApiHandler (endpoints) and
 * {@link DemoAuthenticationProvider} (token resolution).
 */
public interface AuthService {

    /** An authenticated session: the response body of register/login. */
    record Session(long userId, String username, String token) {}

    /** Thrown by register when the username is already taken (maps to 409). */
    class UsernameTakenException extends RuntimeException {
        public UsernameTakenException(String username) {
            super("Username already taken: " + username);
        }
    }

    /**
     * Create a user, fund demo balances, and mint a token.
     *
     * @throws IllegalArgumentException on invalid username/password (400)
     * @throws UsernameTakenException   when the username exists (409)
     * @throws IllegalStateException    when running without persistence (503)
     */
    Session register(String username, String password);

    /**
     * Verify credentials and rotate the active token.
     *
     * @return the new session, or null on unknown username / wrong password (401)
     * @throws IllegalStateException when running without persistence (503)
     */
    Session login(String username, String password);

    /**
     * Resolve a bearer token minted by register/login.
     *
     * @throws AuthenticationException on unknown token or when persistence is off
     */
    Principal authenticateToken(String token) throws AuthenticationException;
}
