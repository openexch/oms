// SPDX-License-Identifier: Apache-2.0
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

        // Register + auto-fund test users. Demo auth (OMS_AUTH_MODE=demo): POST /api/v1/auth/register
        // creates the user, funds demo balances, and mints a bearer token. Orders must carry that
        // token AND the minted userId (the API forbids acting as another user).
        System.out.println("Registering + funding test users...");
        Users users = registerUsers(config);
        System.out.printf("Registered %d users%n", users.ids.length);

        OmsMetricsCollector metrics = new OmsMetricsCollector();
        long totalTarget = (long) config.getTargetRate() * (config.getDurationSeconds() + config.getWarmupSeconds());
        AtomicLong sentCounter = new AtomicLong(0);
        long ratePerThread = config.getTargetRate() / config.getThreads();

        // Start worker threads
        Thread[] workers = new Thread[config.getThreads()];
        for (int i = 0; i < config.getThreads(); i++) {
            workers[i] = new Thread(
                    new HttpOrderPublisher(config, metrics, sentCounter, totalTarget, ratePerThread, users),
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

    /** Registered load-test users: parallel arrays of minted userId + bearer token. */
    static final class Users {
        final long[] ids;
        final String[] tokens;
        Users(long[] ids, String[] tokens) { this.ids = ids; this.tokens = tokens; }
    }

    private static Users registerUsers(OmsLoadConfig config) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        String baseUrl = "http://" + config.getOmsHost() + ":" + config.getOmsPort();
        int n = config.getNumUsers();
        long[] ids = new long[n];
        String[] tokens = new String[n];
        long stamp = System.nanoTime() % 100000000L; // keep username <=20 chars (validation limit)
        for (int i = 0; i < n; i++) {
            String body = String.format("{\"username\":\"ld%d_%d\",\"password\":\"pass1234\"}", stamp, i);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/api/v1/auth/register"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
            String json = resp.body();
            ids[i] = extractLong(json, "userId");
            tokens[i] = extractString(json, "token");
            if (tokens[i] == null) {
                throw new IllegalStateException("register failed (" + resp.statusCode() + "): " + json);
            }
        }
        return new Users(ids, tokens);
    }

    /** Minimal field extractor for the fixed register response {"userId":N,...,"token":"..."}. */
    static long extractLong(String json, String field) {
        int k = json.indexOf("\"" + field + "\":");
        if (k < 0) return -1;
        int s = k + field.length() + 3;
        int e = s;
        while (e < json.length() && (Character.isDigit(json.charAt(e)) || json.charAt(e) == '-')) e++;
        return Long.parseLong(json.substring(s, e));
    }

    static String extractString(String json, String field) {
        int k = json.indexOf("\"" + field + "\":\"");
        if (k < 0) return null;
        int s = k + field.length() + 4;
        int e = json.indexOf('"', s);
        return e < 0 ? null : json.substring(s, e);
    }
}
