// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;
import org.junit.jupiter.api.Test;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;
class PersistenceAvailabilityTest {
    @Test void unobservedAndStaleDatabaseCannotMakeAdmissionReady() {
        var time = new AtomicLong(1);
        var available = new PersistenceAvailability(() -> true, time::get);
        assertFalse(available.getAsBoolean());
        available.run(); assertTrue(available.getAsBoolean());
        time.addAndGet(3_000_000_000L); assertFalse(available.getAsBoolean());
        available.run(); assertTrue(available.getAsBoolean());
    }
    @Test void failureInvalidatesEvidenceAndRequiresANewSuccessfulProbe() {
        var healthy = new AtomicBoolean(true);
        var available = new PersistenceAvailability(() -> {
            if (!healthy.get()) throw new IllegalStateException("DB failed");
            return true;
        });
        available.run(); assertTrue(available.getAsBoolean());
        healthy.set(false); available.run(); assertFalse(available.getAsBoolean());
        healthy.set(true); assertFalse(available.getAsBoolean());
        available.run(); assertTrue(available.getAsBoolean());
    }
}
