// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.loadtest;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Worker thread that generates and POSTs order JSON to the OMS REST API.
 */
public class HttpOrderPublisher implements Runnable {

    private final String baseUrl;
    private final OmsLoadConfig config;
    private final OmsMetricsCollector metrics;
    private final AtomicLong sentCounter;
    private final long targetCount;
    private final long ratePerThread;
    private final HttpClient httpClient;
    private final OmsLoadGenerator.Users users;

    private static final String[] SIDES = {"BUY", "SELL"};
    private static final int[] MARKET_IDS = {1, 2, 3, 4, 5};
    // Midpoints of each market's [minPrice,maxPrice] band, so orders land inside the ±10% price collar.
    private static final double[] REF_PRICES = {50000.0, 3000.0, 100.0, 0.50, 0.10};

    public HttpOrderPublisher(OmsLoadConfig config, OmsMetricsCollector metrics,
                               AtomicLong sentCounter, long targetCount, long ratePerThread,
                               OmsLoadGenerator.Users users) {
        this.baseUrl = "http://" + config.getOmsHost() + ":" + config.getOmsPort();
        this.config = config;
        this.metrics = metrics;
        this.sentCounter = sentCounter;
        this.targetCount = targetCount;
        this.ratePerThread = ratePerThread;
        this.users = users;
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    @Override
    public void run() {
        long intervalNanos = ratePerThread > 0 ? 1_000_000_000L / ratePerThread : 0;

        while (sentCounter.get() < targetCount) {
            long startNanos = System.nanoTime();

            try {
                int u = ThreadLocalRandom.current().nextInt(users.ids.length);
                String json = generateOrderJson(users.ids[u]);
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/api/v1/orders"))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer " + users.tokens[u])
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                long latencyNanos = System.nanoTime() - startNanos;

                sentCounter.incrementAndGet();

                int sc = response.statusCode();
                if (sc == 200 || sc == 201) {
                    metrics.recordSuccess(latencyNanos);   // accepted by OMS -> matched/booked
                } else if (sc == 400) {
                    metrics.recordRejected();               // risk/collar/balance reject
                } else {
                    metrics.recordFailure();                // 401/403/5xx etc.
                }
            } catch (Exception e) {
                sentCounter.incrementAndGet();
                metrics.recordFailure();
            }

            // Rate limiting
            if (intervalNanos > 0) {
                long elapsed = System.nanoTime() - startNanos;
                long sleepNanos = intervalNanos - elapsed;
                if (sleepNanos > 0) {
                    try {
                        Thread.sleep(sleepNanos / 1_000_000, (int) (sleepNanos % 1_000_000));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }

    private String generateOrderJson(long userId) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int marketIdx = rng.nextInt(MARKET_IDS.length);
        int marketId = MARKET_IDS[marketIdx];
        String side = SIDES[rng.nextInt(2)];
        double refPrice = REF_PRICES[marketIdx];

        // Price varies +/- 5% around reference
        double priceFactor = 1.02; // fixed: BUY/SELL cross -> match, funds recycle, stays in collar
        double price = refPrice * priceFactor;
        double quantity = 0.01;

        return String.format(
                "{\"userId\":%d,\"marketId\":%d,\"side\":\"%s\",\"orderType\":\"LIMIT\",\"price\":%.8f,\"quantity\":%.8f}",
                userId, marketId, side, price, quantity);
    }
}
