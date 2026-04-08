package com.openexchange.oms.loadtest;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Main class for OMS HTTP load testing.
 * Spins up worker threads, rate-limits, and prints metrics.
 */
public class OmsLoadGenerator {

    public static void main(String[] args) throws Exception {
        OmsLoadConfig config = OmsLoadConfig.fromArgs(args);

        System.out.println("=== OMS Load Test ===");
        System.out.printf("Host: %s:%d%n", config.getOmsHost(), config.getOmsPort());
        System.out.printf("Target Rate: %d msg/s%n", config.getTargetRate());
        System.out.printf("Duration: %d s (+ %d s warmup)%n", config.getDurationSeconds(), config.getWarmupSeconds());
        System.out.printf("Threads: %d%n", config.getThreads());
        System.out.printf("Users: %d%n", config.getNumUsers());
        System.out.println();

        // Pre-fund test users
        System.out.println("Funding test users...");
        fundTestUsers(config);

        OmsMetricsCollector metrics = new OmsMetricsCollector();
        long totalTarget = (long) config.getTargetRate() * (config.getDurationSeconds() + config.getWarmupSeconds());
        AtomicLong sentCounter = new AtomicLong(0);
        long ratePerThread = config.getTargetRate() / config.getThreads();

        // Start worker threads
        Thread[] workers = new Thread[config.getThreads()];
        for (int i = 0; i < config.getThreads(); i++) {
            workers[i] = new Thread(
                    new HttpOrderPublisher(config, metrics, sentCounter, totalTarget, ratePerThread),
                    "loadtest-worker-" + i);
            workers[i].setDaemon(true);
        }

        long startTime = System.currentTimeMillis();
        for (Thread w : workers) w.start();

        // Warmup phase
        System.out.printf("Warming up for %d seconds...%n", config.getWarmupSeconds());
        Thread.sleep(config.getWarmupSeconds() * 1000L);
        metrics.reset();
        sentCounter.set(0);
        totalTarget = (long) config.getTargetRate() * config.getDurationSeconds();

        // Measurement phase
        System.out.println("Measuring...");
        long endTime = System.currentTimeMillis() + config.getDurationSeconds() * 1000L;
        while (System.currentTimeMillis() < endTime && sentCounter.get() < totalTarget) {
            Thread.sleep(1000);
            metrics.printSnapshot(sentCounter.get());
        }

        // Stop workers
        for (Thread w : workers) w.interrupt();
        for (Thread w : workers) w.join(2000);

        metrics.printFinalReport(sentCounter.get());
    }

    private static void fundTestUsers(OmsLoadConfig config) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        String baseUrl = "http://" + config.getOmsHost() + ":" + config.getOmsPort();

        for (int userId = 1; userId <= config.getNumUsers(); userId++) {
            // Deposit USD (asset 0) - $1,000,000
            String depositJson = "{\"assetId\":0,\"amount\":1000000.0}";
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/api/v1/accounts/" + userId + "/deposit"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(depositJson))
                    .build();
            client.send(req, HttpResponse.BodyHandlers.ofString());

            // Deposit BTC (asset 1)
            depositJson = "{\"assetId\":1,\"amount\":100.0}";
            req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/api/v1/accounts/" + userId + "/deposit"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(depositJson))
                    .build();
            client.send(req, HttpResponse.BodyHandlers.ofString());

            // Deposit ETH (asset 2)
            depositJson = "{\"assetId\":2,\"amount\":1000.0}";
            req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/api/v1/accounts/" + userId + "/deposit"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(depositJson))
                    .build();
            client.send(req, HttpResponse.BodyHandlers.ofString());
        }

        System.out.printf("Funded %d users%n", config.getNumUsers());
    }
}
