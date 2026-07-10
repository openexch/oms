// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import java.util.Locale;

/**
 * Maps {@code OMS_BALANCE_STORE} to a selection outcome (E3/E4).
 *
 * <p>Kept as a tiny pure function — separate from {@link OmsApplication#start()}'s actual store
 * construction, connection, and {@code System.exit} calls — so the env-var-to-behavior mapping can
 * be unit tested without booting Redis, an Aeron cluster, or a JVM-exiting process. The exit paths
 * ({@code redis} probe failure, {@code aeron} readiness/PG-gate failure, unknown value) all still
 * flow through {@link OmsApplication#start()}; this enum only decides WHICH path is taken.</p>
 */
enum BalanceStoreKind {
    REDIS,
    AERON,
    MEMORY,
    /** Anything else — {@link OmsApplication#start()} treats this as FATAL (exit 1). */
    UNKNOWN;

    /** Case/whitespace-tolerant; {@code null} or unrecognized input maps to {@link #UNKNOWN}. */
    static BalanceStoreKind fromConfigValue(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "redis" -> REDIS;
            case "aeron" -> AERON;
            case "memory" -> MEMORY;
            default -> UNKNOWN;
        };
    }
}
