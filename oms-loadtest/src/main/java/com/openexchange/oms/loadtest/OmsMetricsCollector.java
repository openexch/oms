package com.openexchange.oms.loadtest;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Collects and reports performance metrics during load testing.
 */
public class OmsMetricsCollector {

    private final LongAdder successCount = new LongAdder();
    private final LongAdder failureCount = new LongAdder();
    private final LongAdder rejectedCount = new LongAdder();

    private final LatencyTracker latencyTracker = new LatencyTracker();
    private final AtomicLong lastSnapshotTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong lastSnapshotCount = new AtomicLong(0);
    private final long startTime = System.currentTimeMillis();

    public void recordSuccess(long latencyNanos) {
        successCount.increment();
        latencyTracker.record(latencyNanos);
    }

    public void recordFailure() {
        failureCount.increment();
    }

    public void recordRejected() {
        rejectedCount.increment();
    }

    public void reset() {
        successCount.reset();
        failureCount.reset();
        rejectedCount.reset();
        latencyTracker.reset();
        lastSnapshotTime.set(System.currentTimeMillis());
        lastSnapshotCount.set(0);
    }

    public long getSuccessCount() { return successCount.sum(); }
    public long getFailureCount() { return failureCount.sum(); }

    public void printSnapshot(long totalSent) {
        long now = System.currentTimeMillis();
        long lastTime = lastSnapshotTime.getAndSet(now);
        long lastCount = lastSnapshotCount.getAndSet(totalSent);

        long intervalMs = now - lastTime;
        long intervalMessages = totalSent - lastCount;
        double throughput = intervalMs > 0 ? (intervalMessages * 1000.0 / intervalMs) : 0.0;

        long success = successCount.sum();
        long failures = failureCount.sum();
        long rejected = rejectedCount.sum();

        LatencyStats stats = latencyTracker.getStats();

        System.out.printf("| %,8d msg/s | %,10d sent | %,10d ok | %,6d fail | %,6d rej | p50: %6.1fus | p99: %6.1fus |%n",
                (int) throughput, totalSent, success, failures, rejected,
                stats.p50 / 1000.0, stats.p99 / 1000.0);
    }

    public void printFinalReport(long totalSent) {
        long duration = System.currentTimeMillis() - startTime;
        long success = successCount.sum();
        long failures = failureCount.sum();
        long rejected = rejectedCount.sum();

        double avgThroughput = duration > 0 ? (totalSent * 1000.0 / duration) : 0.0;
        LatencyStats stats = latencyTracker.getStats();

        System.out.println();
        System.out.printf("Total Duration:        %,10d ms%n", duration);
        System.out.printf("Messages Sent:         %,10d%n", totalSent);
        System.out.printf("Successful:            %,10d%n", success);
        System.out.printf("Failed:                %,10d%n", failures);
        System.out.printf("Rejected:              %,10d%n", rejected);
        System.out.printf("Average Throughput:    %,10.2f msg/s%n", avgThroughput);
        System.out.println();
        System.out.println("Latency Distribution (us):");
        System.out.printf("  p50:                 %,10.2f us%n", stats.p50 / 1000.0);
        System.out.printf("  p95:                 %,10.2f us%n", stats.p95 / 1000.0);
        System.out.printf("  p99:                 %,10.2f us%n", stats.p99 / 1000.0);
        System.out.printf("  Max:                 %,10.2f us%n", stats.max / 1000.0);
    }

    public record LatencyStats(long min, long max, long avg, long p50, long p95, long p99) {}

    private static class LatencyTracker {
        private static final int CAPACITY = 1_000_000;
        private final long[] samples = new long[CAPACITY];
        private volatile int writeIndex = 0;

        public void record(long latencyNanos) {
            int idx = writeIndex;
            samples[idx % CAPACITY] = latencyNanos;
            writeIndex = idx + 1;
        }

        public void reset() {
            writeIndex = 0;
            Arrays.fill(samples, 0);
        }

        public LatencyStats getStats() {
            int currentIndex = writeIndex;
            if (currentIndex == 0) return new LatencyStats(0, 0, 0, 0, 0, 0);

            int count = Math.min(currentIndex, CAPACITY);
            long[] copy = new long[count];
            int startIdx = currentIndex > CAPACITY ? currentIndex - CAPACITY : 0;
            for (int i = 0; i < count; i++) {
                copy[i] = samples[(startIdx + i) % CAPACITY];
            }

            Arrays.sort(copy);
            long sum = 0;
            for (long v : copy) sum += v;

            return new LatencyStats(
                    copy[0], copy[count - 1], sum / count,
                    copy[(int) (count * 0.50)],
                    copy[(int) (count * 0.95)],
                    copy[Math.min(count - 1, (int) (count * 0.99))]);
        }
    }
}
