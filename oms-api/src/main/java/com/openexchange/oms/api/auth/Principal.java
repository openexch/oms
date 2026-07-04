// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.auth;

import java.util.Set;

/**
 * Authenticated caller identity, derived by an {@link AuthenticationProvider}.
 *
 * @param subject provider-specific identity (API-key name, JWT sub, "dev")
 * @param userId  the exchange user this principal acts as by default
 * @param roles   granted roles; see {@link #ROLE_ADMIN} and {@link #ROLE_ANY_USER}
 */
public record Principal(String subject, long userId, Set<String> roles) {

    /** Grants access to the /api/v1/admin/* endpoints. */
    public static final String ROLE_ADMIN = "ADMIN";

    /**
     * Grants acting on behalf of ANY userId (dev provider, trusted service
     * integrations such as load generators). Without it a principal may only
     * act as its own {@link #userId}.
     */
    public static final String ROLE_ANY_USER = "ANY_USER";

    public boolean hasRole(String role) {
        return roles.contains(role);
    }

    /** Whether this principal may act as the given user. */
    public boolean canActAs(long requestedUserId) {
        return userId == requestedUserId || roles.contains(ROLE_ANY_USER);
    }

    /** The userId to act as when the caller supplied none (0 = unspecified). */
    public long resolveUserId(long requestedUserId) {
        return requestedUserId == 0 ? userId : requestedUserId;
    }
}
