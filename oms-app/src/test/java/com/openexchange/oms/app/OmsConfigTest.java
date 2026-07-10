// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pins {@link OmsConfig#loadDefaults()} parsing of the E3/E4 balance-store knobs
 * (OMS_BALANCE_STORE / OMS_AE_HOLD_TIMEOUT_MS / OMS_AE_ACK_TIMEOUT_MS / OMS_AE_CONNECT_TIMEOUT_MS):
 * defaults, explicit overrides, and garbage-input failure.
 *
 * <p>{@code OmsConfig.prop()} checks {@code System.getenv(KEY)} first, falling back to the system
 * property {@code KEY.toLowerCase().replace('_','.')}. Overriding real env vars isn't possible
 * from a plain JUnit test, so these tests drive the system-property fallback instead — every
 * property set here is cleared in {@link #tearDown()} so no test bleeds into another in the same
 * forked JVM.</p>
 */
class OmsConfigTest {

    private static final String BALANCE_STORE_PROP = "oms.balance.store";
    private static final String HOLD_TIMEOUT_PROP = "oms.ae.hold.timeout.ms";
    private static final String ACK_TIMEOUT_PROP = "oms.ae.ack.timeout.ms";
    private static final String CONNECT_TIMEOUT_PROP = "oms.ae.connect.timeout.ms";

    @AfterEach
    void tearDown() {
        System.clearProperty(BALANCE_STORE_PROP);
        System.clearProperty(HOLD_TIMEOUT_PROP);
        System.clearProperty(ACK_TIMEOUT_PROP);
        System.clearProperty(CONNECT_TIMEOUT_PROP);
    }

    @Test
    void defaultsWhenUnset() {
        OmsConfig config = OmsConfig.loadDefaults();
        assertEquals("redis", config.balanceStore());
        assertEquals(250L, config.aeHoldTimeoutMs());
        assertEquals(1000L, config.aeAckTimeoutMs());
        assertEquals(30_000L, config.aeConnectTimeoutMs());
    }

    @Test
    void balanceStoreOverride() {
        System.setProperty(BALANCE_STORE_PROP, "aeron");
        assertEquals("aeron", OmsConfig.loadDefaults().balanceStore());
    }

    @Test
    void holdTimeoutOverride() {
        System.setProperty(HOLD_TIMEOUT_PROP, "77");
        assertEquals(77L, OmsConfig.loadDefaults().aeHoldTimeoutMs());
    }

    @Test
    void ackTimeoutOverride() {
        System.setProperty(ACK_TIMEOUT_PROP, "4321");
        assertEquals(4321L, OmsConfig.loadDefaults().aeAckTimeoutMs());
    }

    @Test
    void connectTimeoutOverride() {
        System.setProperty(CONNECT_TIMEOUT_PROP, "60000");
        assertEquals(60_000L, OmsConfig.loadDefaults().aeConnectTimeoutMs());
    }

    @Test
    void garbageNumericValueThrows() {
        System.setProperty(HOLD_TIMEOUT_PROP, "not-a-number");
        assertThrows(NumberFormatException.class, OmsConfig::loadDefaults);
    }

    @Test
    void garbageBalanceStoreValueIsAcceptedByConfigParsing() {
        // OmsConfig is a dumb string carrier; validating "unknown store name" is
        // BalanceStoreKind's job (see BalanceStoreKindTest), not OmsConfig's.
        System.setProperty(BALANCE_STORE_PROP, "mongodb");
        assertEquals("mongodb", OmsConfig.loadDefaults().balanceStore());
    }
}
