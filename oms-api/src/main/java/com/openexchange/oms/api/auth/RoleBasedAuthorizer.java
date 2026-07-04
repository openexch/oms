// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.auth;

/** Default policy: ADMIN role gates admin actions; act-as is own userId or ANY_USER role. */
public final class RoleBasedAuthorizer implements Authorizer {

    @Override
    public boolean allow(Principal principal, String action, String resource) {
        if (principal == null) return false;
        return switch (action) {
            case ACTION_ADMIN -> principal.hasRole(Principal.ROLE_ADMIN);
            case ACTION_ACT_AS_USER -> {
                try {
                    yield principal.canActAs(Long.parseLong(resource));
                } catch (NumberFormatException e) {
                    yield false;
                }
            }
            default -> false;
        };
    }
}
