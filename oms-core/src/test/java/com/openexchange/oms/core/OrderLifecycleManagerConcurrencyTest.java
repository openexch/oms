// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.core;

import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OrderSide;
import com.openexchange.oms.common.enums.OmsOrderType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression for oms#70: registerOrder runs on Netty I/O threads while
 * cluster-egress transitions run on the OMS core thread. With the previous
 * non-thread-safe Agrona maps, concurrent structural modification corrupted
 * the probe chain into an INFINITE LOOP (both HTTP workers spun forever and
 * the REST plane died). This test hammers the same access pattern; under
 * the old maps it reliably hangs or corrupts within a few runs — the
 * @Timeout converts the historical infinite loop into a failure.
 */
class OrderLifecycleManagerConcurrencyTest {

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void concurrentRegisterAndEgressTransitionsDoNotCorrupt() throws Exception {
        OrderLifecycleManager lcm = new OrderLifecycleManager();
        int writers = 4;
        int perWriter = 20_000;
        AtomicLong ids = new AtomicLong(1);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(writers + 1);
        ExecutorService pool = Executors.newFixedThreadPool(writers + 1);

        // Netty-thread shape: register + read back.
        for (int w = 0; w < writers; w++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perWriter; i++) {
                        long id = ids.getAndIncrement();
                        lcm.registerOrder(order(id));
                        lcm.getOrder(id);
                        lcm.findActiveByClientOrderId(id % 50, "c" + id);
                    }
                } catch (InterruptedException ignored) {
                } finally {
                    done.countDown();
                }
            });
        }
        // Core-thread shape: egress transitions that remove terminal orders.
        pool.submit(() -> {
            try {
                start.await();
                long cursor = 1;
                while (done.getCount() > 1 || cursor < ids.get()) {
                    // CANCELLED (status 3) removes the order when present.
                    lcm.onClusterOrderStatus(cursor, 0, 3, 0, 0);
                    cursor++;
                    if (cursor > ids.get()) {
                        cursor = Math.max(1, ids.get() - 1000);
                        if (done.getCount() <= 1) break;
                    }
                }
            } catch (Throwable ignored) {
            } finally {
                done.countDown();
            }
        });

        start.countDown();
        assertTrue(done.await(50, TimeUnit.SECONDS), "workers wedged (map corruption?)");
        pool.shutdownNow();

        // Drain everything the egress thread didn't reach; the maps must
        // still be structurally sound end-to-end.
        long max = ids.get();
        for (long id = 1; id < max; id++) {
            lcm.onClusterOrderStatus(id, 0, 3, 0, 0);
        }
        assertEquals(0, lcm.getActiveOrderCount(), "all orders must be removable after the storm");
    }

    private static OmsOrder order(long id) {
        OmsOrder o = new OmsOrder();
        o.setOmsOrderId(id);
        o.setUserId(id % 50);
        o.setMarketId(1);
        o.setSide(OrderSide.BUY);
        o.setOrderType(OmsOrderType.LIMIT);
        o.setPrice(100_00000000L);
        o.setQuantity(1_00000000L);
        o.setClientOrderId("c" + id);
        return o;
    }
}
