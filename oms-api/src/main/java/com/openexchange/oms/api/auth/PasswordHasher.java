// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.auth;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * PBKDF2 password hashing (JDK-only, no new dependency). Stored form is
 * self-describing so parameters can be raised later without invalidating
 * existing hashes: {@code pbkdf2$<iterations>$<saltB64>$<hashB64>}.
 *
 * Demo-grade iteration count: verification runs off the Netty event loop
 * (see the auth executor in RestApiHandler) but registration/login latency
 * should still stay well under a second.
 */
public final class PasswordHasher {

    private static final String ALGORITHM = "PBKDF2WithHmacSHA256";
    private static final int ITERATIONS = 50_000;
    private static final int SALT_BYTES = 16;
    private static final int HASH_BITS = 256;

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final Base64.Encoder B64E = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder B64D = Base64.getUrlDecoder();

    private PasswordHasher() {
    }

    public static String hash(String password) {
        byte[] salt = new byte[SALT_BYTES];
        RANDOM.nextBytes(salt);
        byte[] hash = pbkdf2(password, salt, ITERATIONS);
        return "pbkdf2$" + ITERATIONS + "$" + B64E.encodeToString(salt) + "$" + B64E.encodeToString(hash);
    }

    /** Constant-time verification. False on any malformed stored value. */
    public static boolean verify(String password, String stored) {
        try {
            String[] parts = stored.split("\\$");
            if (parts.length != 4 || !parts[0].equals("pbkdf2")) {
                return false;
            }
            int iterations = Integer.parseInt(parts[1]);
            byte[] salt = B64D.decode(parts[2]);
            byte[] expected = B64D.decode(parts[3]);
            byte[] actual = pbkdf2(password, salt, iterations);
            return MessageDigest.isEqual(expected, actual);
        } catch (Exception e) {
            return false;
        }
    }

    private static byte[] pbkdf2(String password, byte[] salt, int iterations) {
        try {
            PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, HASH_BITS);
            return SecretKeyFactory.getInstance(ALGORITHM).generateSecret(spec).getEncoded();
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException("PBKDF2 unavailable", e);
        }
    }

    /** Mint an opaque URL-safe bearer token (32 random bytes). */
    public static String newToken() {
        byte[] bytes = new byte[32];
        RANDOM.nextBytes(bytes);
        return B64E.encodeToString(bytes);
    }
}
