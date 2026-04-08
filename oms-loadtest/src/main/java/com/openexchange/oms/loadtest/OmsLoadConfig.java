package com.openexchange.oms.loadtest;

/**
 * Configuration for OMS load testing.
 */
public class OmsLoadConfig {

    private int targetRate = 1000;
    private int durationSeconds = 30;
    private int threads = 4;
    private String omsHost = "localhost";
    private int omsPort = 8080;
    private String scenario = "mixed";
    private int warmupSeconds = 5;
    private int numUsers = 10;

    public static OmsLoadConfig fromArgs(String[] args) {
        OmsLoadConfig config = new OmsLoadConfig();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--rate" -> config.targetRate = Integer.parseInt(args[++i]);
                case "--duration" -> config.durationSeconds = Integer.parseInt(args[++i]);
                case "--threads" -> config.threads = Integer.parseInt(args[++i]);
                case "--host" -> config.omsHost = args[++i];
                case "--port" -> config.omsPort = Integer.parseInt(args[++i]);
                case "--scenario" -> config.scenario = args[++i];
                case "--warmup" -> config.warmupSeconds = Integer.parseInt(args[++i]);
                case "--users" -> config.numUsers = Integer.parseInt(args[++i]);
            }
        }
        return config;
    }

    public int getTargetRate() { return targetRate; }
    public int getDurationSeconds() { return durationSeconds; }
    public int getThreads() { return threads; }
    public String getOmsHost() { return omsHost; }
    public int getOmsPort() { return omsPort; }
    public String getScenario() { return scenario; }
    public int getWarmupSeconds() { return warmupSeconds; }
    public int getNumUsers() { return numUsers; }
}
