// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.core;

import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.persistence.PositionAggregate;
import com.openexchange.oms.risk.RiskEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Rebuilds in-memory OMS state from the Postgres ledger on startup (oms#35).
 *
 * Open orders repopulate the lifecycle manager (as-is, no transitions), risk
 * open-order slots, and the synthetic engine's trigger/iceberg monitoring.
 * Positions replay from the persisted executions aggregate. Balances need no
 * rebuild (Redis-backed, survives restart).
 *
 * A persisted PENDING_HOLD can precede a successful external reservation whose
 * acknowledgement was lost. Pre-cluster intents therefore block startup until
 * explicit recovery; skipping them would erase an unresolved liability.
 */
public final class StartupStateRebuilder {

    private static final Logger log = LoggerFactory.getLogger(StartupStateRebuilder.class);

    private StartupStateRebuilder() {
    }

    /** What the rebuild did, for logging and test assertions. */
    public record Result(int ordersRestored, int ordersSkippedPreCluster, int syntheticsRegistered,
                         int positionsRestored) {
    }

    public static Result rebuild(
            List<OmsOrder> openOrders,
            List<PositionAggregate> positions,
            OrderLifecycleManager lifecycleManager,
            SyntheticOrderEngine syntheticEngine,
            RiskEngine riskEngine) {

        for (OmsOrder order : openOrders) {
            if (order.getStatus() == OmsOrderStatus.PENDING_RISK || order.getStatus() == OmsOrderStatus.PENDING_HOLD) {
                throw new IllegalStateException("Unresolved durable admission intent: " + order.getOmsOrderId());
            }
        }
        int restored = 0;
        int skipped = 0;
        int synthetics = 0;

        for (OmsOrder order : openOrders) {
            lifecycleManager.restoreOrder(order);
            riskEngine.onOrderOpened(order.getUserId());
            restored++;

            switch (order.getOrderType()) {
                case STOP_LOSS, STOP_LIMIT, TRAILING_STOP, ICEBERG -> {
                    syntheticEngine.registerOrder(order);
                    synthetics++;
                }
                default -> {
                }
            }
        }

        for (PositionAggregate p : positions) {
            riskEngine.restorePosition(p.userId(), p.marketId(), p.netQuantity());
        }

        Result result = new Result(restored, skipped, synthetics, positions.size());
        log.info("State rebuilt from Postgres: {} open orders restored ({} synthetic re-armed), " +
                        "{} pre-cluster limbo orders skipped, {} positions restored",
                result.ordersRestored(), result.syntheticsRegistered(),
                result.ordersSkippedPreCluster(), result.positionsRestored());
        return result;
    }
}
