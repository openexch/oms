// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.core;

import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.common.enums.OmsOrderType;
import com.openexchange.oms.common.enums.OrderSide;
import com.openexchange.oms.common.enums.TimeInForce;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase-1 money-state GUARDRAIL: SyntheticOrderEngine thread-safety (SYN-1 / oms#70 class).
 * <p>
 * register/removeOrder run on Netty I/O threads (createOrder/cancelOrder), while the evaluate
 * and onIcebergSliceFilled paths run on the OMS core thread. The engine's maps are non-thread-safe
 * (Agrona + TreeMap), so concurrent structural modification used to corrupt an Agrona probe chain
 * into an infinite loop (the twice-seen total REST outage, oms#70). This hammers that exact access
 * pattern; the timeout converts the historical hang into a failure. It must complete cleanly with
 * the lock in place. A re-entrant callback (removeOrder from the iceberg refill callback) also
 * proves callbacks run OUTSIDE the lock without self-deadlock.
 */
class Phase1SyntheticConcurrencyGuardrailTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void concurrentRegisterEvaluateRemoveDoNotCorrupt() throws Exception {
        SyntheticOrderEngine syn = new SyntheticOrderEngine();
        syn.setTriggerCallback((order, childType, childPrice) -> { /* no-op */ });
        // Refill callback re-enters removeOrder: exercises "callbacks run outside the lock".
        syn.setIcebergCallback((iceberg, nextSliceQty) -> syn.removeOrder(iceberg));

        int writers = 4;
        int perWriter = 20_000;
        AtomicLong ids = new AtomicLong(1);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(writers + 1);
        ExecutorService pool = Executors.newFixedThreadPool(writers + 1);

        // Netty-thread shape: register a mix of synthetic types, then remove.
        for (int w = 0; w < writers; w++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perWriter; i++) {
                        long id = ids.getAndIncrement();
                        OmsOrder o = order(id, (int) (id % 3));
                        syn.registerOrder(o);
                        syn.removeOrder(o);
                    }
                } catch (InterruptedException ignored) {
                } finally {
                    done.countDown();
                }
            });
        }
        // Core-thread shape: market-data evaluation + iceberg refills over the live id range.
        pool.submit(() -> {
            try {
                start.await();
                long cursor = 1;
                while (done.getCount() > 1) {
                    syn.onMarketDataUpdate(1, 50_000_00000000L, 50_010_00000000L);
                    syn.onIcebergSliceFilled(cursor++);
                    if (cursor > ids.get()) cursor = 1;
                }
            } catch (Throwable ignored) {
            } finally {
                done.countDown();
            }
        });

        start.countDown();
        assertTrue(done.await(50, TimeUnit.SECONDS),
                "workers wedged — synthetic-engine map corruption under concurrent access?");
        pool.shutdownNow();

        // Structurally sound end-to-end: every count reads without throwing/looping.
        syn.getActiveIcebergCount();
        syn.getActiveStopCount();
        syn.getActiveTrailingCount();
    }

    private static OmsOrder order(long id, int typeIdx) {
        OmsOrder o = new OmsOrder();
        o.setOmsOrderId(id);
        o.setUserId(id % 50);
        o.setMarketId(1);
        o.setTimeInForce(TimeInForce.GTC);
        o.setPrice(50_000_00000000L);
        o.setQuantity(10_00000000L);
        switch (typeIdx) {
            case 0 -> {
                o.setOrderType(OmsOrderType.ICEBERG);
                o.setSide(OrderSide.SELL);
                o.setDisplayQuantity(2_00000000L);
                o.setHiddenQuantity(10_00000000L);
                o.setStatus(OmsOrderStatus.NEW);
            }
            case 1 -> {
                o.setOrderType(OmsOrderType.STOP_LOSS);
                o.setSide(OrderSide.SELL);
                o.setStopPrice(50_000_00000000L);
                o.setStatus(OmsOrderStatus.PENDING_TRIGGER);
            }
            default -> {
                o.setOrderType(OmsOrderType.TRAILING_STOP);
                o.setSide(OrderSide.BUY);
                o.setTrailingDelta(500_00000000L);
                o.setStatus(OmsOrderStatus.PENDING_TRIGGER);
            }
        }
        return o;
    }
}
