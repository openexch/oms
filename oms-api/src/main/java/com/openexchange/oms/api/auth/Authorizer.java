// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.auth;

/**
 * SPI: decide whether a principal may perform an action on a resource.
 * Default policy is {@link RoleBasedAuthorizer}; integrators can substitute
 * their own (e.g. per-market permissions, read-only keys).
 */
public interface Authorizer {

    /** Admin endpoints (risk config, circuit breakers). Resource = request URI/method. */
    String ACTION_ADMIN = "admin";

    /** Acting on behalf of a user (orders, balances, deposit/withdraw). Resource = userId. */
    String ACTION_ACT_AS_USER = "act-as-user";

    boolean allow(Principal principal, String action, String resource);
}
