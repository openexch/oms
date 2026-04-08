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

    private static final String[] SIDES = {"BUY", "SELL"};
    private static final int[] MARKET_IDS = {1, 2, 3, 4, 5};
    private static final double[] REF_PRICES = {50000.0, 3000.0, 100.0, 0.50, 0.10};

    public HttpOrderPublisher(OmsLoadConfig config, OmsMetricsCollector metrics,
                               AtomicLong sentCounter, long targetCount, long ratePerThread) {
        this.baseUrl = "http://" + config.getOmsHost() + ":" + config.getOmsPort();
        this.config = config;
        this.metrics = metrics;
        this.sentCounter = sentCounter;
        this.targetCount = targetCount;
        this.ratePerThread = ratePerThread;
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
                String json = generateOrderJson();
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/api/v1/orders"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                long latencyNanos = System.nanoTime() - startNanos;

                sentCounter.incrementAndGet();

                if (response.statusCode() == 201) {
                    metrics.recordSuccess(latencyNanos);
                } else if (response.statusCode() == 400) {
                    metrics.recordRejected();
                } else {
                    metrics.recordFailure();
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

    private String generateOrderJson() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        long userId = rng.nextLong(1, config.getNumUsers() + 1);
        int marketIdx = rng.nextInt(MARKET_IDS.length);
        int marketId = MARKET_IDS[marketIdx];
        String side = SIDES[rng.nextInt(2)];
        double refPrice = REF_PRICES[marketIdx];

        // Price varies +/- 5% around reference
        double priceFactor = 0.95 + rng.nextDouble() * 0.10;
        double price = refPrice * priceFactor;
        double quantity = 0.001 + rng.nextDouble() * 0.1;

        return String.format(
                "{\"userId\":%d,\"marketId\":%d,\"side\":\"%s\",\"orderType\":\"LIMIT\",\"price\":%.8f,\"quantity\":%.8f}",
                userId, marketId, side, price, quantity);
    }
}
