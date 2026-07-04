// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.auth;

import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

class JwtAuthenticationProviderTest {

    private static final String SECRET = "test-secret-please-rotate";
    private static final long NOW_MS = 1_751_600_000_000L;

    private final JwtAuthenticationProvider provider = new JwtAuthenticationProvider(SECRET, () -> NOW_MS);

    private static String b64(String json) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(json.getBytes(StandardCharsets.UTF_8));
    }

    private static String sign(String headerAndPayload, String secret) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(mac.doFinal(headerAndPayload.getBytes(StandardCharsets.US_ASCII)));
    }

    private static String token(String headerJson, String payloadJson, String secret) throws Exception {
        String head = b64(headerJson) + "." + b64(payloadJson);
        return head + "." + sign(head, secret);
    }

    @Test
    void validTokenYieldsPrincipal() throws Exception {
        String jwt = token("{\"alg\":\"HS256\",\"typ\":\"JWT\"}",
                "{\"sub\":\"42\",\"roles\":[\"admin\"],\"exp\":" + (NOW_MS / 1000 + 60) + "}", SECRET);
        Principal p = provider.authenticate(TestHeaders.of("Authorization", "Bearer " + jwt));
        assertEquals(42, p.userId());
        assertTrue(p.hasRole(Principal.ROLE_ADMIN), "roles are uppercased");
        assertFalse(p.canActAs(43));
    }

    @Test
    void expiredTokenRejected() throws Exception {
        String jwt = token("{\"alg\":\"HS256\"}",
                "{\"sub\":\"42\",\"exp\":" + (NOW_MS / 1000 - 1) + "}", SECRET);
        assertThrows(AuthenticationException.class,
                () -> provider.authenticate(TestHeaders.of("Authorization", "Bearer " + jwt)));
    }

    @Test
    void wrongSignatureRejected() throws Exception {
        String jwt = token("{\"alg\":\"HS256\"}", "{\"sub\":\"42\"}", "other-secret");
        assertThrows(AuthenticationException.class,
                () -> provider.authenticate(TestHeaders.of("Authorization", "Bearer " + jwt)));
    }

    @Test
    void algNoneRejected() throws Exception {
        String head = b64("{\"alg\":\"none\"}") + "." + b64("{\"sub\":\"42\"}");
        String jwt = head + "." + sign(head, SECRET);
        assertThrows(AuthenticationException.class,
                () -> provider.authenticate(TestHeaders.of("Authorization", "Bearer " + jwt)));
    }

    @Test
    void nonNumericSubRejected() throws Exception {
        String jwt = token("{\"alg\":\"HS256\"}", "{\"sub\":\"alice\"}", SECRET);
        assertThrows(AuthenticationException.class,
                () -> provider.authenticate(TestHeaders.of("Authorization", "Bearer " + jwt)));
    }

    @Test
    void malformedAndMissingRejected() {
        assertThrows(AuthenticationException.class,
                () -> provider.authenticate(TestHeaders.of("Authorization", "Bearer not.a-jwt")));
        assertThrows(AuthenticationException.class, () -> provider.authenticate(TestHeaders.of()));
    }

    @Test
    void blankSecretRefusedAtConstruction() {
        assertThrows(IllegalArgumentException.class, () -> new JwtAuthenticationProvider(""));
    }
}
