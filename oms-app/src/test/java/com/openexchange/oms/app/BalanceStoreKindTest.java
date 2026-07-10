// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins the {@code OMS_BALANCE_STORE} value -> {@link BalanceStoreKind} mapping WITHOUT booting
 * anything (no Redis, no Aeron cluster, no {@code System.exit}): {@link OmsApplication#start()}
 * routes each {@link BalanceStoreKind} to its own FATAL-vs-proceed behavior, but that routing is
 * exercised manually / operationally, not here — this test only pins the selection logic.
 */
class BalanceStoreKindTest {

    @Test
    void redisMapsToRedis() {
        assertEquals(BalanceStoreKind.REDIS, BalanceStoreKind.fromConfigValue("redis"));
    }

    @Test
    void aeronMapsToAeron() {
        assertEquals(BalanceStoreKind.AERON, BalanceStoreKind.fromConfigValue("aeron"));
    }

    @Test
    void memoryMapsToMemory() {
        assertEquals(BalanceStoreKind.MEMORY, BalanceStoreKind.fromConfigValue("memory"));
    }

    @Test
    void unknownValueMapsToUnknown() {
        assertEquals(BalanceStoreKind.UNKNOWN, BalanceStoreKind.fromConfigValue("mongodb"));
        assertEquals(BalanceStoreKind.UNKNOWN, BalanceStoreKind.fromConfigValue(""));
        assertEquals(BalanceStoreKind.UNKNOWN, BalanceStoreKind.fromConfigValue("redi5"));
    }

    @Test
    void nullMapsToUnknown() {
        assertEquals(BalanceStoreKind.UNKNOWN, BalanceStoreKind.fromConfigValue(null));
    }

    @Test
    void caseAndWhitespaceInsensitive() {
        assertEquals(BalanceStoreKind.REDIS, BalanceStoreKind.fromConfigValue("REDIS"));
        assertEquals(BalanceStoreKind.AERON, BalanceStoreKind.fromConfigValue(" Aeron "));
        assertEquals(BalanceStoreKind.MEMORY, BalanceStoreKind.fromConfigValue("Memory"));
    }
}
