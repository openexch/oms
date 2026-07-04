package com.openexchange.oms.api.auth;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DevAuthenticationProviderTest {

    private final DevAuthenticationProvider provider = new DevAuthenticationProvider();

    @Test
    void noCredentialsYieldsDefaultDevPrincipal() throws Exception {
        Principal p = provider.authenticate(TestHeaders.of());
        assertEquals(DevAuthenticationProvider.DEFAULT_USER_ID, p.userId());
        assertTrue(p.hasRole(Principal.ROLE_ADMIN));
        assertTrue(p.hasRole(Principal.ROLE_ANY_USER));
        assertTrue(p.canActAs(42), "dev principal must keep caller-supplied userIds working");
    }

    @Test
    void devTokenSelectsUser() throws Exception {
        Principal p = provider.authenticate(TestHeaders.of("Authorization", "Bearer dev:42"));
        assertEquals(42, p.userId());
        assertEquals(42, p.resolveUserId(0));
    }

    @Test
    void devTokenViaApiKeyHeader() throws Exception {
        Principal p = provider.authenticate(TestHeaders.of("X-API-Key", "dev:9"));
        assertEquals(9, p.userId());
    }

    @Test
    void malformedDevTokenFallsBackToDefault() throws Exception {
        Principal p = provider.authenticate(TestHeaders.of("Authorization", "Bearer dev:abc"));
        assertEquals(DevAuthenticationProvider.DEFAULT_USER_ID, p.userId());
    }
}
