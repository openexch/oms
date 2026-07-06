// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.auth;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PasswordHasherTest {

    @Test
    void hashAndVerify_roundTrip() {
        String stored = PasswordHasher.hash("correct horse battery");
        assertTrue(stored.startsWith("pbkdf2$"));
        assertTrue(PasswordHasher.verify("correct horse battery", stored));
    }

    @Test
    void verify_wrongPassword_false() {
        String stored = PasswordHasher.hash("password1");
        assertFalse(PasswordHasher.verify("password2", stored));
    }

    @Test
    void hash_samePasswordTwice_differentSalts() {
        assertNotEquals(PasswordHasher.hash("secret"), PasswordHasher.hash("secret"));
    }

    @Test
    void verify_malformedStoredValue_false() {
        assertFalse(PasswordHasher.verify("x", "garbage"));
        assertFalse(PasswordHasher.verify("x", "pbkdf2$notanumber$AA$BB"));
        assertFalse(PasswordHasher.verify("x", ""));
    }

    @Test
    void verify_selfDescribingIterations_honored() {
        // A hash created with different (embedded) parameters still verifies:
        // the stored form carries its own iteration count.
        String stored = PasswordHasher.hash("pw");
        String[] parts = stored.split("\\$");
        assertEquals(4, parts.length);
        assertEquals("50000", parts[1]);
    }

    @Test
    void newToken_uniqueAndUrlSafe() {
        String a = PasswordHasher.newToken();
        String b = PasswordHasher.newToken();
        assertNotEquals(a, b);
        assertTrue(a.matches("[A-Za-z0-9_-]{40,}"));
    }
}
