package com.openexchange.oms.api.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * HS256 JWT verifier (OMS_AUTH_MODE=jwt). No external JWT dependency — HMAC
 * via javax.crypto, payload via Jackson.
 *
 * Claims: {@code sub} = numeric userId (required); {@code roles} = string
 * array (optional); {@code exp} = epoch seconds (enforced when present).
 * Only alg HS256 is accepted — "none" and everything else is rejected.
 */
public final class JwtAuthenticationProvider implements AuthenticationProvider {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Base64.Decoder B64 = Base64.getUrlDecoder();

    private final byte[] secret;
    private final LongSupplier clockMs;

    public JwtAuthenticationProvider(String secret) {
        this(secret, System::currentTimeMillis);
    }

    JwtAuthenticationProvider(String secret, LongSupplier clockMs) {
        if (secret == null || secret.isBlank()) {
            throw new IllegalArgumentException("OMS_JWT_SECRET is required for OMS_AUTH_MODE=jwt");
        }
        this.secret = secret.getBytes(StandardCharsets.UTF_8);
        this.clockMs = clockMs;
    }

    @Override
    public Principal authenticate(Headers headers) throws AuthenticationException {
        String token = AuthenticationProvider.extractToken(headers);
        if (token == null || token.isEmpty()) {
            throw new AuthenticationException("Missing credentials (Authorization: Bearer <jwt>)");
        }
        String[] parts = token.split("\\.");
        if (parts.length != 3) {
            throw new AuthenticationException("Malformed JWT");
        }
        try {
            JsonNode header = MAPPER.readTree(B64.decode(parts[0]));
            if (!"HS256".equals(header.path("alg").asText())) {
                throw new AuthenticationException("Unsupported JWT alg (only HS256)");
            }

            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            byte[] expected = mac.doFinal((parts[0] + "." + parts[1]).getBytes(StandardCharsets.US_ASCII));
            if (!MessageDigest.isEqual(expected, B64.decode(parts[2]))) {
                throw new AuthenticationException("Invalid JWT signature");
            }

            JsonNode payload = MAPPER.readTree(B64.decode(parts[1]));
            if (payload.has("exp") && payload.get("exp").asLong() * 1000 <= clockMs.getAsLong()) {
                throw new AuthenticationException("JWT expired");
            }
            String sub = payload.path("sub").asText("");
            long userId;
            try {
                userId = Long.parseLong(sub);
            } catch (NumberFormatException e) {
                throw new AuthenticationException("JWT sub must be a numeric userId");
            }
            Set<String> roles = new HashSet<>();
            if (payload.has("roles") && payload.get("roles").isArray()) {
                for (JsonNode role : payload.get("roles")) {
                    roles.add(role.asText().toUpperCase());
                }
            }
            return new Principal("jwt-user-" + userId, userId, Set.copyOf(roles));
        } catch (AuthenticationException e) {
            throw e;
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("HmacSHA256 unavailable", e);
        } catch (Exception e) {
            throw new AuthenticationException("Malformed JWT: " + e.getMessage());
        }
    }
}
