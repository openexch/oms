// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.api.auth;

/** Credentials missing, malformed, expired, or unknown — maps to HTTP 401 / gRPC UNAUTHENTICATED. */
public class AuthenticationException extends Exception {

    public AuthenticationException(String message) {
        super(message);
    }
}
