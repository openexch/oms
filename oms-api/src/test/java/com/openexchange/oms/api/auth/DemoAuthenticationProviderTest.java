// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.auth;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Demo-mode auth: registered-user tokens resolve via AuthService with
 * self-scoped principals; the dev-token backdoor is restricted to userId 1
 * and the sim range 900000-900999 and is itself self-scoped (no ANY_USER),
 * so no dev token can act on a registered user's data.
 */
class DemoAuthenticationProviderTest {

    private static final AuthService FAKE = new AuthService() {
        @Override
        public Session register(String username, String password) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Session login(String username, String password) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Principal authenticateToken(String token) throws AuthenticationException {
            if ("valid-token".equals(token)) {
                return new Principal("user-alice", 100123, Set.of());
            }
            throw new AuthenticationException("Invalid or expired token");
        }
    };

    private static AuthenticationProvider.Headers bearer(String token) {
        return name -> name.equalsIgnoreCase("Authorization") ? "Bearer " + token : null;
    }

    private final DemoAuthenticationProvider provider = new DemoAuthenticationProvider(FAKE);

    @Test
    void registeredToken_resolvesSelfScopedPrincipal() throws Exception {
        Principal p = provider.authenticate(bearer("valid-token"));
        assertEquals(100123, p.userId());
        assertEquals("user-alice", p.subject());
        assertTrue(p.canActAs(100123));
        assertFalse(p.canActAs(1));
        assertFalse(p.hasRole(Principal.ROLE_ADMIN));
    }

    @Test
    void unknownToken_rejected() {
        assertThrows(AuthenticationException.class, () -> provider.authenticate(bearer("nope")));
    }

    @Test
    void missingToken_rejected() {
        assertThrows(AuthenticationException.class, () -> provider.authenticate(name -> null));
    }

    @Test
    void devToken_opsUser1_adminButSelfScoped() throws Exception {
        Principal p = provider.authenticate(bearer("dev:1"));
        assertEquals(1, p.userId());
        assertTrue(p.hasRole(Principal.ROLE_ADMIN));
        assertTrue(p.canActAs(1));
        // No ANY_USER: even the ops backdoor cannot act on other users' data
        assertFalse(p.canActAs(100123));
    }

    @Test
    void devToken_simRange_selfScopedNoRoles() throws Exception {
        Principal bot = provider.authenticate(bearer("dev:900001"));
        assertEquals(900001, bot.userId());
        assertTrue(bot.roles().isEmpty());
        assertTrue(bot.canActAs(900001));
        assertFalse(bot.canActAs(900002));

        Principal canary = provider.authenticate(bearer("dev:900999"));
        assertEquals(900999, canary.userId());
    }

    @Test
    void devToken_registeredRange_rejected() {
        assertThrows(AuthenticationException.class, () -> provider.authenticate(bearer("dev:100123")));
        assertThrows(AuthenticationException.class, () -> provider.authenticate(bearer("dev:2")));
        assertThrows(AuthenticationException.class, () -> provider.authenticate(bearer("dev:899999")));
    }

    @Test
    void devToken_malformed_rejected() {
        assertThrows(AuthenticationException.class, () -> provider.authenticate(bearer("dev:abc")));
    }

    @Test
    void withoutAuthService_userTokensRejected_devBackdoorStillWorks() throws Exception {
        DemoAuthenticationProvider noDb = new DemoAuthenticationProvider(null);
        assertThrows(AuthenticationException.class, () -> noDb.authenticate(bearer("valid-token")));
        assertEquals(1, noDb.authenticate(bearer("dev:1")).userId());
    }
}
