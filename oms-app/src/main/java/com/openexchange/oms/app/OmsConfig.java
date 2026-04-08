package com.openexchange.oms.app;

/**
 * OMS application configuration.
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
    int nodeId
) {
    public static OmsConfig loadDefaults() {
        return new OmsConfig(
            intProp("OMS_HTTP_PORT", 8080),
            intProp("OMS_GRPC_PORT", 9090),
            prop("OMS_REDIS_HOST", "localhost"),
            intProp("OMS_REDIS_PORT", 6379),
            prop("OMS_POSTGRES_URL", "jdbc:postgresql://localhost:5432/oms"),
            prop("OMS_POSTGRES_USER", "oms"),
            prop("OMS_POSTGRES_PASSWORD", "oms"),
            prop("OMS_CLUSTER_INGRESS", "localhost:9000"),
            intProp("OMS_NODE_ID", 0)
        );
    }

    private static String prop(String key, String defaultValue) {
        String env = System.getenv(key);
        return env != null ? env : System.getProperty(key.toLowerCase().replace('_', '.'), defaultValue);
    }

    private static int intProp(String key, int defaultValue) {
        String val = prop(key, null);
        return val != null ? Integer.parseInt(val) : defaultValue;
    }
}
