package com.openexchange.oms.api.auth;

/**
 * SPI: derive a {@link Principal} from transport headers.
 *
 * Implementations are wired once at startup (see OMS_AUTH_MODE) and invoked on
 * every REST request, WebSocket upgrade, and gRPC call. Integrators plug their
 * own IAM by implementing this interface; the OMS ships a dev no-op
 * ({@link DevAuthenticationProvider}), a static API-key registry
 * ({@link ApiKeyAuthenticationProvider}), and an HS256 JWT verifier
 * ({@link JwtAuthenticationProvider}).
 */
public interface AuthenticationProvider {

    /** Transport-agnostic header accessor. Names are case-insensitive; null when absent. */
    @FunctionalInterface
    interface Headers {
        String get(String name);
    }

    /**
     * @return the authenticated principal (never null)
     * @throws AuthenticationException when credentials are missing or invalid
     */
    Principal authenticate(Headers headers) throws AuthenticationException;

    /** Extract the bearer token or API key from standard headers, or null when absent. */
    static String extractToken(Headers headers) {
        String auth = headers.get("Authorization");
        if (auth != null && auth.regionMatches(true, 0, "Bearer ", 0, 7)) {
            return auth.substring(7).trim();
        }
        String apiKey = headers.get("X-API-Key");
        return apiKey != null ? apiKey.trim() : null;
    }
}
