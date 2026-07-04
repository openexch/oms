// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.auth;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class PrincipalTest {

    @Test
    void ownUserOnlyWithoutAnyUserRole() {
        Principal p = new Principal("k1", 7, Set.of());
        assertTrue(p.canActAs(7));
        assertFalse(p.canActAs(8));
    }

    @Test
    void anyUserRoleActsAsEveryone() {
        Principal p = new Principal("svc", 1, Set.of(Principal.ROLE_ANY_USER));
        assertTrue(p.canActAs(1));
        assertTrue(p.canActAs(999_999));
    }

    @Test
    void resolveUserIdDefaultsToPrincipal() {
        Principal p = new Principal("k1", 7, Set.of());
        assertEquals(7, p.resolveUserId(0));
        assertEquals(3, p.resolveUserId(3));
    }

    @Test
    void roleBasedAuthorizer() {
        Authorizer authz = new RoleBasedAuthorizer();
        Principal admin = new Principal("a", 1, Set.of(Principal.ROLE_ADMIN));
        Principal user = new Principal("u", 2, Set.of());

        assertTrue(authz.allow(admin, Authorizer.ACTION_ADMIN, "PUT /api/v1/admin/risk/config/1"));
        assertFalse(authz.allow(user, Authorizer.ACTION_ADMIN, "PUT /api/v1/admin/risk/config/1"));
        assertTrue(authz.allow(user, Authorizer.ACTION_ACT_AS_USER, "2"));
        assertFalse(authz.allow(user, Authorizer.ACTION_ACT_AS_USER, "3"));
        assertFalse(authz.allow(user, Authorizer.ACTION_ACT_AS_USER, "not-a-number"));
        assertFalse(authz.allow(null, Authorizer.ACTION_ACT_AS_USER, "2"));
    }
}
