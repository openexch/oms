package com.openexchange.oms.api.auth;

import java.util.Set;

/**
 * Dev-only no-op provider (OMS_AUTH_MODE=dev): accepts EVERY request.
 *
 * No credentials → user 1 with ADMIN + ANY_USER, which keeps the demo UI and
 * the load harness (caller-supplied userIds) working unchanged. A
 * "dev:&lt;userId&gt;" bearer token / API key selects a different default user.
 * Never enable outside development.
 */
public final class DevAuthenticationProvider implements AuthenticationProvider {

    public static final long DEFAULT_USER_ID = 1;

    private static final Set<String> DEV_ROLES = Set.of(Principal.ROLE_ADMIN, Principal.ROLE_ANY_USER);

    @Override
    public Principal authenticate(Headers headers) {
        String token = AuthenticationProvider.extractToken(headers);
        if (token != null && token.startsWith("dev:")) {
            try {
                long userId = Long.parseLong(token.substring(4));
                return new Principal("dev-user-" + userId, userId, DEV_ROLES);
            } catch (NumberFormatException ignored) {
                // fall through to the default dev principal
            }
        }
        return new Principal("dev", DEFAULT_USER_ID, DEV_ROLES);
    }
}
