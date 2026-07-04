// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.auth;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ApiKeyAuthenticationProviderTest {

    @Test
    void inlineSpecWithRoles() throws Exception {
        var provider = ApiKeyAuthenticationProvider.parse(
                "alpha-key:7;ops-key:1:ADMIN|ANY_USER", "");
        assertFalse(provider.isEmpty());

        Principal user = provider.authenticate(TestHeaders.of("X-API-Key", "alpha-key"));
        assertEquals(7, user.userId());
        assertFalse(user.hasRole(Principal.ROLE_ADMIN));
        assertFalse(user.canActAs(8));

        Principal ops = provider.authenticate(TestHeaders.of("Authorization", "Bearer ops-key"));
        assertEquals(1, ops.userId());
        assertTrue(ops.hasRole(Principal.ROLE_ADMIN));
        assertTrue(ops.canActAs(12345));
    }

    @Test
    void missingCredentialsRejected() {
        var provider = ApiKeyAuthenticationProvider.parse("alpha-key:7", "");
        assertThrows(AuthenticationException.class, () -> provider.authenticate(TestHeaders.of()));
    }

    @Test
    void unknownKeyRejected() {
        var provider = ApiKeyAuthenticationProvider.parse("alpha-key:7", "");
        assertThrows(AuthenticationException.class,
                () -> provider.authenticate(TestHeaders.of("X-API-Key", "wrong")));
    }

    @Test
    void emptyRegistryRejectsEverything() {
        var provider = ApiKeyAuthenticationProvider.parse("", "");
        assertTrue(provider.isEmpty());
        assertThrows(AuthenticationException.class,
                () -> provider.authenticate(TestHeaders.of("X-API-Key", "anything")));
    }

    @Test
    void keyFileWithCommentsAndBlankLines(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("keys");
        Files.writeString(file, "# ops\nfile-key:33\n\n# eof\n");
        var provider = ApiKeyAuthenticationProvider.parse("", file.toString());
        assertEquals(33, provider.authenticate(TestHeaders.of("X-API-Key", "file-key")).userId());
    }

    @Test
    void badEntryFailsFast() {
        assertThrows(IllegalArgumentException.class,
                () -> ApiKeyAuthenticationProvider.parse("just-a-key-no-user", ""));
        assertThrows(IllegalArgumentException.class,
                () -> ApiKeyAuthenticationProvider.parse("", "/nonexistent/keys"));
    }
}
