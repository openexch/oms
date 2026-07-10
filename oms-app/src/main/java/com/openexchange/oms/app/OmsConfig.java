// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

/**
 * OMS application configuration.
 *
 * Auth (oms#36): authMode selects the AuthenticationProvider — "api-key"
 * (secure default; with no keys configured every request is rejected),
 * "jwt" (HS256, requires jwtSecret), or "dev" (accepts everything — must be
 * opted into explicitly, never production).
 *
 * Balance store (E3/E4): balanceStore selects the {@code BalanceStore} —
 * "redis" (default; a failed connectivity probe is FATAL, no silent
 * in-memory fallback), "aeron" (the AE-backed money store; also gates on
 * PG availability and boot-time balance-projection readiness), or "memory"
 * (explicit non-durable opt-in, dev/test only). The AE tunables
 * (aeHoldTimeoutMs/aeAckTimeoutMs/aeConnectTimeoutMs) only apply when
 * balanceStore=aeron.
 */
public record OmsConfig(
    int httpPort,
    int grpcPort,
    String redisHost,
    int redisPort,
    String postgresUrl,
    String postgresUser,
    String postgresPassword,
    String clusterIngress,
    int nodeId,
    String authMode,
    String apiKeys,
    String apiKeysFile,
    String jwtSecret,
    String corsOrigins,
    String auditLogPath,
    String balanceStore,
    long aeHoldTimeoutMs,
    long aeAckTimeoutMs,
    long aeConnectTimeoutMs
) {
    public static OmsConfig loadDefaults() {
        return new OmsConfig(
            intProp("OMS_HTTP_PORT", 8080),
            intProp("OMS_GRPC_PORT", 9090),
            prop("OMS_REDIS_HOST", "localhost"),
            intProp("OMS_REDIS_PORT", 6379),
            prop("OMS_POSTGRES_URL", "jdbc:postgresql://localhost:5432/oms"),
            prop("OMS_POSTGRES_USER", "oms"),
            // No default password (oms#37): unset means Postgres auth fails and
            // the OMS runs without persistence rather than shipping "oms/oms".
            secretProp("OMS_POSTGRES_PASSWORD", ""),
            prop("OMS_CLUSTER_INGRESS", "localhost:9000"),
            intProp("OMS_NODE_ID", 0),
            prop("OMS_AUTH_MODE", "api-key"),
            prop("OMS_API_KEYS", ""),
            prop("OMS_API_KEYS_FILE", ""),
            secretProp("OMS_JWT_SECRET", ""),
            prop("OMS_CORS_ORIGINS", ""),
            prop("OMS_AUDIT_LOG", "oms-audit.log"),
            prop("OMS_BALANCE_STORE", "redis"),
            longProp("OMS_AE_HOLD_TIMEOUT_MS", 250L),
            longProp("OMS_AE_ACK_TIMEOUT_MS", 1000L),
            longProp("OMS_AE_CONNECT_TIMEOUT_MS", 30_000L)
        );
    }

    private static String prop(String key, String defaultValue) {
        String env = System.getenv(key);
        return env != null ? env : System.getProperty(key.toLowerCase().replace('_', '.'), defaultValue);
    }

    /** Like prop, but also accepts KEY_FILE pointing at a secret file (oms#37). */
    private static String secretProp(String key, String defaultValue) {
        String direct = prop(key, null);
        if (direct != null) return direct;
        String file = prop(key + "_FILE", null);
        if (file != null && !file.isBlank()) {
            try {
                return java.nio.file.Files.readString(java.nio.file.Path.of(file)).trim();
            } catch (java.io.IOException e) {
                throw new IllegalArgumentException("Cannot read " + key + "_FILE " + file + ": " + e.getMessage(), e);
            }
        }
        return defaultValue;
    }

    private static int intProp(String key, int defaultValue) {
        String val = prop(key, null);
        return val != null ? Integer.parseInt(val) : defaultValue;
    }

    private static long longProp(String key, long defaultValue) {
        String val = prop(key, null);
        return val != null ? Long.parseLong(val) : defaultValue;
    }
}
