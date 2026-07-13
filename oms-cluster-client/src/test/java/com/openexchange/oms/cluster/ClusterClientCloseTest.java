// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.cluster;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the reconnect-path close discipline of {@link ClusterClient#closeWithFallback}:
 * a graceful {@code AeronCluster.close()} that throws mid-way (session-close send on a broken
 * session) must still release the underlying owned Aeron client via the fallback, and no
 * failure may ever propagate into the polling loop. Uses plain {@link AutoCloseable}s; the
 * real close paths need a live cluster, so only the ordering/fallback contract is unit-tested
 * here (live verification signal: the {@code images=} count in the STATS log line).
 */
class ClusterClientCloseTest {

    @Test
    void gracefulCloseSucceedsFallbackNotInvoked() {
        List<String> calls = new ArrayList<>();

        ClusterClient.closeWithFallback(
                () -> calls.add("graceful"),
                () -> calls.add("fallback"),
                "test");

        assertEquals(List.of("graceful"), calls);
    }

    @Test
    void gracefulCloseThrowsFallbackInvokedAfterIt() {
        List<String> calls = new ArrayList<>();

        ClusterClient.closeWithFallback(
                () -> {
                    calls.add("graceful");
                    throw new IllegalStateException("session-close send failed");
                },
                () -> calls.add("fallback"),
                "test");

        assertEquals(List.of("graceful", "fallback"), calls);
    }

    @Test
    void bothClosesThrowNothingPropagates() {
        assertDoesNotThrow(() -> ClusterClient.closeWithFallback(
                () -> {
                    throw new IllegalStateException("graceful boom");
                },
                () -> {
                    throw new IllegalStateException("fallback boom");
                },
                "test"));
    }

    @Test
    void nullGracefulIsNoopAndSkipsFallback() {
        List<String> calls = new ArrayList<>();

        ClusterClient.closeWithFallback(null, () -> calls.add("fallback"), "test");

        assertEquals(List.of(), calls);
    }

    @Test
    void nullFallbackWithThrowingGracefulDoesNotThrow() {
        assertDoesNotThrow(() -> ClusterClient.closeWithFallback(
                () -> {
                    throw new IllegalStateException("graceful boom");
                },
                null,
                "test"));
    }

    /**
     * Repeated invocation delegates idempotence to the closeable itself, matching
     * {@code AeronCluster.close()} which is a no-op once closed: the helper adds no state of
     * its own and simply invokes the target again.
     */
    @Test
    void repeatedInvocationDelegatesIdempotenceToTarget() {
        List<String> calls = new ArrayList<>();
        AutoCloseable idempotent = new AutoCloseable() {
            private boolean closed = false;

            @Override
            public void close() {
                if (!closed) {
                    closed = true;
                    calls.add("closed");
                }
            }
        };

        ClusterClient.closeWithFallback(idempotent, () -> calls.add("fallback"), "test");
        ClusterClient.closeWithFallback(idempotent, () -> calls.add("fallback"), "test");

        assertEquals(List.of("closed"), calls);
    }
}
