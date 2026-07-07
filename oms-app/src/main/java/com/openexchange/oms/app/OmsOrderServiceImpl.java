// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.match.domain.FixedPoint;
import com.openexchange.oms.api.OrderService;
import com.openexchange.oms.api.dto.*;
import com.openexchange.oms.cluster.ClusterClient;
import com.openexchange.oms.cluster.OrderSubmission;
import com.openexchange.oms.common.domain.*;
import com.openexchange.oms.common.enums.*;
import com.openexchange.oms.core.OmsCoreEngine;
import com.openexchange.oms.core.OrderLifecycleManager;
import com.openexchange.oms.ledger.BalanceStore;
import com.openexchange.oms.ledger.LedgerEntry;
import com.openexchange.oms.ledger.LedgerService;
import com.openexchange.oms.risk.RiskEngine;
import com.openexchange.oms.risk.RiskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Implementation of OrderService that wires API requests to the core engine.
 * Runs on Netty I/O threads — performs risk checks and ledger holds inline,
 * then enqueues to the cluster client MPSC queue.
 */
public class OmsOrderServiceImpl implements OrderService {

    private static final Logger log = LoggerFactory.getLogger(OmsOrderServiceImpl.class);

    private final OmsCoreEngine coreEngine;
    private final RiskEngine riskEngine;
    private final LedgerService ledgerService;
    private final ClusterClient clusterClient;
    private final BalanceStore balanceStore;
    private final OmsEgressAdapter egressAdapter;
    private final SnowflakeIdGenerator idGenerator;
    private final OmsMarketDataProvider marketDataProvider;

    // Slippage multiplier used to estimate an effective price (5% above best ask) for BUY
    // orders that carry no real limit price (MARKET, STOP_LOSS, TRAILING_STOP) — both for
    // the ledger hold and, for MARKET, the eventual cluster submission.
    private static final double MARKET_BUY_SLIPPAGE = 1.05;

    // Optional admission-stage timers (oms#38); no-ops until a registry is set.
    private io.micrometer.core.instrument.Timer riskCheckTimer;
    private io.micrometer.core.instrument.Timer ledgerHoldTimer;

    // Optional Postgres repositories (oms#40): history reads + terminal-order
    // fallbacks. Null when the OMS runs without persistence.
    private com.openexchange.oms.persistence.PostgresOrderRepository orderRepository;
    private com.openexchange.oms.persistence.PostgresExecutionRepository executionRepository;

    public void setRepositories(com.openexchange.oms.persistence.PostgresOrderRepository orderRepository,
                                com.openexchange.oms.persistence.PostgresExecutionRepository executionRepository) {
        this.orderRepository = orderRepository;
        this.executionRepository = executionRepository;
    }

    public void setMeterRegistry(io.micrometer.core.instrument.MeterRegistry registry) {
        this.riskCheckTimer = io.micrometer.core.instrument.Timer.builder("oms_risk_check_seconds")
                .description("Risk engine admission check latency")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
        this.ledgerHoldTimer = io.micrometer.core.instrument.Timer.builder("oms_ledger_hold_seconds")
                .description("Ledger hold placement latency (Redis round-trip)")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
    }

    public OmsOrderServiceImpl(OmsCoreEngine coreEngine, RiskEngine riskEngine,
                                LedgerService ledgerService, ClusterClient clusterClient,
                                BalanceStore balanceStore, OmsEgressAdapter egressAdapter,
                                SnowflakeIdGenerator idGenerator,
                                OmsMarketDataProvider marketDataProvider) {
        this.coreEngine = coreEngine;
        this.riskEngine = riskEngine;
        this.ledgerService = ledgerService;
        this.clusterClient = clusterClient;
        this.balanceStore = balanceStore;
        this.egressAdapter = egressAdapter;
        this.idGenerator = idGenerator;
        this.marketDataProvider = marketDataProvider;
    }

    @Override
    public CreateOrderResponse createOrder(CreateOrderRequest request) {
        try {
            // Validate
            if (request.getUserId() <= 0) {
                return CreateOrderResponse.rejected("Invalid userId");
            }
            if (request.getMarketId() < 1 || request.getMarketId() > 5) {
                return CreateOrderResponse.rejected("Invalid marketId");
            }
            if (request.getClientOrderId() != null && request.getClientOrderId().length() > 64) {
                return CreateOrderResponse.rejected("clientOrderId too long (max 64)");
            }

            // clientOrderId idempotency (oms#40): if the caller already has an
            // ACTIVE order under this id, return it instead of creating a new
            // one — a retried POST after a lost response is a no-op. The id
            // becomes reusable once that order is terminal.
            if (request.getClientOrderId() != null && !request.getClientOrderId().isEmpty()) {
                long existingId = coreEngine.getLifecycleManager()
                        .findActiveByClientOrderId(request.getUserId(), request.getClientOrderId());
                if (existingId != 0) {
                    OmsOrder existing = coreEngine.getLifecycleManager().getOrder(existingId);
                    String existingStatus = existing != null ? existing.getStatus().name() : "UNKNOWN";
                    return CreateOrderResponse.duplicate(existingId, existingStatus);
                }
            }

            // Parse enums
            OrderSide side = OrderSide.valueOf(request.getSide().toUpperCase());
            OmsOrderType orderType = OmsOrderType.valueOf(request.getOrderType().toUpperCase());
            TimeInForce tif = request.getTimeInForce() != null
                ? TimeInForce.valueOf(request.getTimeInForce().toUpperCase())
                : TimeInForce.GTC;

            // Create OMS order
            OmsOrder order = new OmsOrder();
            order.setOmsOrderId(idGenerator.nextId());
            order.setUserId(request.getUserId());
            order.setMarketId(request.getMarketId());
            order.setSide(side);
            order.setOrderType(orderType);
            order.setTimeInForce(tif);
            // Request money is already exact fixed-point (oms#39): the wire
            // carries decimal strings, parsed at the Jackson boundary.
            order.setPrice(request.getPrice());
            order.setQuantity(request.getQuantity());
            order.setRemainingQty(order.getQuantity());
            order.setStopPrice(request.getStopPrice());
            order.setTrailingDelta(request.getTrailingDelta());
            order.setDisplayQuantity(request.getDisplayQuantity());
            order.setExpiresAtMs(request.getExpiresAtMs());
            order.setClientOrderId(request.getClientOrderId());

            if (orderType == OmsOrderType.ICEBERG) {
                order.setHiddenQuantity(order.getQuantity());
                // Normalize displayQuantity ONCE at creation so every later slice
                // computation (the first slice below, and every refill slice in
                // SyntheticOrderEngine.onIcebergSliceFilled) reads one consistent
                // value (oms#82). Missing/zero or a value >= the total quantity
                // both mean "fully displayed" — a single slice for the whole
                // order, i.e. iceberg degenerates to a plain limit order.
                long requestedDisplay = order.getDisplayQuantity();
                order.setDisplayQuantity(requestedDisplay > 0
                        ? Math.min(requestedDisplay, order.getQuantity())
                        : order.getQuantity());
            }

            // 1. Register with lifecycle manager → PENDING_RISK
            OrderLifecycleManager lcm = coreEngine.getLifecycleManager();
            lcm.registerOrder(order);

            // 2. Risk check
            long riskStart = System.nanoTime();
            RiskResult riskResult = riskEngine.check(
                    order.getUserId(), order.getMarketId(), side, orderType,
                    order.getPrice(), order.getQuantity());
            if (riskCheckTimer != null) {
                riskCheckTimer.record(System.nanoTime() - riskStart, java.util.concurrent.TimeUnit.NANOSECONDS);
            }

            if (!riskResult.isPassed()) {
                lcm.onRiskRejected(order.getOmsOrderId(), riskResult.getRejectReason());
                return CreateOrderResponse.rejected(riskResult.getRejectReason());
            }

            // 3. Risk passed → PENDING_HOLD
            lcm.onRiskPassed(order.getOmsOrderId());

            // 3b. For BUY orders with no real limit price (price=0 is the "unset" convention
            // for MARKET, STOP_LOSS, TRAILING_STOP), estimate an effective price from best ask
            // BEFORE placing the ledger hold below. This MUST happen before step 4: holdForOrder
            // computes holdAmount = FixedPoint.multiply(order.getPrice(), quantity), and price=0
            // always yields holdAmount=0, which fails LedgerService's holdAmount<=0 guard and
            // rejects the order as "Insufficient balance" regardless of the account's real funds
            // (oms#81) — the risk check above already estimates via best ask and passes, so the
            // hold and the risk check disagreed purely because of this ordering. STOP_LIMIT is
            // unaffected: it always carries a real user-supplied price, so it never hits this
            // branch, and SELL orders hold base asset by quantity so they don't need a price at all.
            if (side == OrderSide.BUY && order.getPrice() <= 0) {
                long bestAsk = marketDataProvider.getBestAsk(order.getMarketId());
                if (bestAsk <= 0) {
                    lcm.onHoldFailed(order.getOmsOrderId(), "No liquidity (no best ask)");
                    return CreateOrderResponse.rejected("No liquidity available for market buy");
                }
                // Use best ask with slippage buffer as the estimated price for holding
                long estimatedPrice = (long) (bestAsk * MARKET_BUY_SLIPPAGE);
                order.setPrice(estimatedPrice);
            }

            // 4. Place ledger hold
            long holdStart = System.nanoTime();
            List<LedgerEntry> holdEntries = ledgerService.holdForOrder(order);
            if (ledgerHoldTimer != null) {
                ledgerHoldTimer.record(System.nanoTime() - holdStart, java.util.concurrent.TimeUnit.NANOSECONDS);
            }
            if (holdEntries.isEmpty()) {
                lcm.onHoldFailed(order.getOmsOrderId(), "Insufficient balance");
                return CreateOrderResponse.rejected("Insufficient balance");
            }

            // 5. Hold placed → PENDING_NEW
            lcm.onHoldPlaced(order.getOmsOrderId());

            // 6. Synthetic orders go to PENDING_TRIGGER — EXCEPT icebergs.
            //
            // A stop/trailing order is dormant: it waits for a market-data trigger
            // before anything touches the book, so PENDING_TRIGGER is correct. An
            // ICEBERG has no trigger condition — it must start working its slices
            // immediately. Routing it through PENDING_TRIGGER with no evaluator
            // left it resting there forever with the full hold locked and nothing
            // on the book (oms#82). Instead, register it for refill tracking and
            // submit the FIRST display slice now, through the SAME path every
            // refill uses (coreEngine.submitIcebergSlice → the submitIcebergSlice
            // cluster handler). The order stays PENDING_NEW and advances to NEW on
            // the cluster ack — the ordinary resting-order path, not PENDING_TRIGGER.
            if (orderType == OmsOrderType.ICEBERG) {
                // State is already set up for the refill cycle: hiddenQuantity =
                // total (set at construction) and displayQuantity normalized to
                // min(requested, total). onIcebergSliceFilled subtracts a full
                // displayQuantity from hiddenQuantity on each fill, so the first
                // slice must be exactly this size for the arithmetic to line up.
                coreEngine.getSyntheticEngine().registerOrder(order);
                long firstSlice = Math.min(order.getDisplayQuantity(), order.getQuantity());
                // No re-hold: the full-quantity hold placed in step 4 covers every
                // slice; submitIcebergSlice only enqueues to the cluster (exactly
                // like a refill), it never touches the ledger. Double-holding here
                // would lock the funds twice.
                coreEngine.submitIcebergSlice(order, firstSlice);
                riskEngine.onOrderOpened(order.getUserId());
                return CreateOrderResponse.accepted(order.getOmsOrderId(), order.getStatus().name());
            }

            if (orderType.isSynthetic()) {
                lcm.onPendingTrigger(order.getOmsOrderId());
                coreEngine.getSyntheticEngine().registerOrder(order);
                return CreateOrderResponse.accepted(order.getOmsOrderId(), order.getStatus().name());
            }

            // 7. Build and submit to cluster
            OrderSubmission submission = getOrderSubmission(orderType, side, order);

            // Track the open-order slot BEFORE the enqueue so the open/close pair stays
            // balanced whichever way submit goes (oms#85). On a queue-full reject the state
            // listener fires riskEngine.onOrderClosed for the PENDING_NEW→REJECTED transition;
            // with onOrderOpened still deferred to after a successful submit, that close had no
            // matching open and would decrement one of the user's OTHER open-order slots.
            riskEngine.onOrderOpened(order.getUserId());

            boolean enqueued = clusterClient.submitOrder(submission);
            if (!enqueued) {
                // Queue full — terminalize the order the cluster never saw (oms#85). Do NOT
                // release the hold or close the slot here: the state listener performs BOTH on
                // the PENDING_NEW→REJECTED transition (and pushes the terminal to WS/gRPC).
                // Releasing here too double-releases the hold, which can succeed against the
                // user's OTHER locked funds — money creation. This is the double-release trap.
                lcm.onSubmitFailed(order.getOmsOrderId(), "Order queue full");
                return CreateOrderResponse.rejected("Order queue full");
            }

            return CreateOrderResponse.accepted(order.getOmsOrderId(), order.getStatus().name());

        } catch (IllegalArgumentException e) {
            return CreateOrderResponse.rejected(e.getMessage());
        }
    }

    private static OrderSubmission getOrderSubmission(OmsOrderType orderType, OrderSide side, OmsOrder order) {
        com.match.infrastructure.generated.OrderType sbeOrderType = mapOrderType(orderType);
        com.match.infrastructure.generated.OrderSide sbeOrderSide = mapOrderSide(side);
        // Exact since match#30. Risk checks reject overflowing notionals before
        // this point on the normal path; converting to IllegalArgumentException
        // routes any residual case into the caller's rejected-response path.
        long totalPrice;
        try {
            totalPrice = FixedPoint.multiply(order.getPrice(), order.getQuantity());
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Order notional overflows fixed-point");
        }

        return OrderSubmission.createOrder(
                order.getUserId(), order.getMarketId(),
                order.getPrice(), order.getQuantity(), totalPrice,
                sbeOrderType, sbeOrderSide, order.getOmsOrderId());
    }

    @Override
    public CancelOrderResponse cancelOrder(long omsOrderId) {
        OrderLifecycleManager lcm = coreEngine.getLifecycleManager();
        OmsOrder order = lcm.onCancelRequested(omsOrderId);
        if (order == null) {
            return CancelOrderResponse.rejected("Order not found or already terminal");
        }

        // If order is PENDING_TRIGGER (synthetic), remove from synthetic engine.
        // Hold + slot release happens in the state listener on the CANCELLED
        // transition below — releasing here too double-credited the hold once
        // the listener learned to release PENDING_TRIGGER terminals (oms#49).
        if (order.getStatus() == OmsOrderStatus.PENDING_TRIGGER) {
            coreEngine.getSyntheticEngine().removeOrder(order);
            lcm.onClusterOrderStatus(omsOrderId, 0, 3, 0, 0); // status 3 = CANCELLED
            return CancelOrderResponse.accepted(omsOrderId);
        }

        // If order has been sent to cluster, send cancel
        if (order.getClusterOrderId() != 0) {
            OrderSubmission cancelSubmission = OrderSubmission.cancelOrder(
                    order.getUserId(), order.getClusterOrderId(), order.getMarketId());
            clusterClient.submitOrder(cancelSubmission);
        } else if (order.getStatus() == OmsOrderStatus.PENDING_NEW
                || order.getStatus() == OmsOrderStatus.NEW
                || order.getStatus() == OmsOrderStatus.PARTIALLY_FILLED) {
            // clusterOrderId not yet received from egress — order is in-flight
            log.warn("Cancel requested but clusterOrderId not yet assigned for omsOrderId={}, status={}",
                    omsOrderId, order.getStatus());
            return CancelOrderResponse.rejected("Order is in-flight, please retry shortly");
        }

        return CancelOrderResponse.accepted(omsOrderId);
    }

    @Override
    public Map<String, Object> updateOrder(long omsOrderId, long newPrice, long newQuantity) {
        OrderLifecycleManager lcm = coreEngine.getLifecycleManager();
        OmsOrder order = lcm.getOrder(omsOrderId);
        if (order == null) {
            return Map.of("accepted", false, "message", "Order not found");
        }
        if (order.getStatus().isTerminal()) {
            return Map.of("accepted", false, "message", "Order is already terminal");
        }
        if (order.getClusterOrderId() == 0) {
            return Map.of("accepted", false, "message", "Order is in-flight, please retry shortly");
        }
        // Amend is defined for plain resting limit orders only. Synthetic parents
        // (stop/trailing) live as PENDING_TRIGGER with clusterOrderId==0 and are caught
        // above; icebergs span multiple cluster slices, so amending "the" leg is
        // incoherent — reject explicitly (oms#67).
        if (order.getOrderType() != OmsOrderType.LIMIT && order.getOrderType() != OmsOrderType.LIMIT_MAKER) {
            return Map.of("accepted", false, "message",
                    "Amend is only supported for LIMIT/LIMIT_MAKER orders");
        }
        if (order.isCancelRequested()) {
            return Map.of("accepted", false, "message", "Cancel already in progress");
        }

        long price = newPrice > 0 ? newPrice : order.getPrice();
        long quantity = newQuantity > 0 ? newQuantity : order.getQuantity();

        // quantity is the amended TOTAL (prior fills included): the engine's
        // cancel-and-replace rests the FULL submitted leg quantity, so the leg carries
        // the remaining part only. An amend at or below what already filled is void.
        long legQuantity = quantity - order.getFilledQty();
        if (legQuantity <= 0) {
            return Map.of("accepted", false, "message",
                    "Amended quantity is not above the already-filled quantity");
        }

        // Risk-safe hold adjustment (oms#67): a GROWN notional is held now — this is the
        // amend's funds check. A SHRUNK notional releases only at resolution (the amend
        // may still fail, and the original order must stay fully held until it does).
        long holdTarget = ledgerService.computeAmendHoldTarget(order, price, quantity);
        if (holdTarget < 0) {
            return Map.of("accepted", false, "message", "Amended notional overflows");
        }
        long holdDelta = holdTarget - order.getHoldAmount();

        // Claim the replace marker BEFORE moving funds: it is the one-amend-at-a-time
        // gate, and no egress for this replace can exist until submitOrder below.
        // pendingHoldDelta starts at 0 and is set only AFTER the incremental hold
        // actually lands — an abort must never release funds that were never held.
        if (!lcm.onReplaceSubmitted(omsOrderId, price, quantity, 0, holdTarget)) {
            return Map.of("accepted", false, "message", "Amend already in progress");
        }

        if (holdDelta > 0) {
            if (ledgerService.holdAmendDelta(order, holdDelta).isEmpty()) {
                lcm.abortReplace(order, "insufficient balance for the amend delta");
                return Map.of("accepted", false, "message", "Insufficient balance for amend");
            }
            order.setPendingHoldDelta(holdDelta);
        }

        com.match.infrastructure.generated.OrderSide sbeOrderSide = mapOrderSide(order.getSide());
        com.match.infrastructure.generated.OrderType sbeOrderType = mapOrderType(order.getOrderType());

        OrderSubmission submission = OrderSubmission.updateOrder(
                order.getUserId(), order.getClusterOrderId(), order.getMarketId(),
                price, legQuantity, sbeOrderType, sbeOrderSide);

        boolean enqueued = clusterClient.submitOrder(submission);
        if (!enqueued) {
            lcm.abortReplace(order, "submission queue full");
            return Map.of("accepted", false, "message", "Order queue full");
        }

        // id as a string, like every DTO (oms#39)
        return Map.of("accepted", true, "omsOrderId", String.valueOf(omsOrderId), "message", "Update submitted");
    }

    @Override
    public OrderResponse getOrder(long omsOrderId) {
        OmsOrder order = coreEngine.getLifecycleManager().getOrder(omsOrderId);
        if (order != null) {
            return OrderResponse.fromOrder(order);
        }
        // Terminal orders leave the active map; Postgres keeps them (oms#40).
        if (orderRepository != null) {
            OmsOrder persisted = orderRepository.findById(omsOrderId);
            if (persisted != null) {
                return OrderResponse.fromOrder(persisted);
            }
        }
        return null;
    }

    @Override
    public List<OrderResponse> queryOrders(long userId, String status) {
        // Terminal statuses are never in the active map — serve them from
        // Postgres (oms#40). Without persistence, [] (the old behavior).
        if (status != null && isTerminalStatus(status)) {
            if (orderRepository == null) {
                return new ArrayList<>();
            }
            return getOrderHistory(userId, status, 100, 0);
        }

        OrderLifecycleManager lcm = coreEngine.getLifecycleManager();
        List<OrderResponse> results = new ArrayList<>();

        // Scan active orders - this uses the Agrona map's values iterator
        lcm.forEachActiveOrder(order -> {
            if (order.getUserId() == userId) {
                if (status == null || order.getStatus().name().equals(status)) {
                    results.add(OrderResponse.fromOrder(order));
                }
            }
        });

        return results;
    }

    private static boolean isTerminalStatus(String status) {
        try {
            return OmsOrderStatus.valueOf(status).isTerminal();
        } catch (IllegalArgumentException e) {
            return false; // unknown string: matches nothing in the active scan, as before
        }
    }

    @Override
    public List<OrderResponse> getOrderHistory(long userId, String status, int limit, int offset) {
        requirePersistence();
        int boundedLimit = boundLimit(limit);
        int boundedOffset = Math.max(0, offset);
        List<OmsOrder> orders;
        if (status == null || status.isEmpty()) {
            orders = orderRepository.findByUser(userId, boundedLimit, boundedOffset);
        } else {
            // throws IllegalArgumentException on an unknown status → 400 at the edge
            OmsOrderStatus parsed = OmsOrderStatus.valueOf(status);
            orders = orderRepository.findByUserAndStatus(userId, parsed, boundedLimit, boundedOffset);
        }
        List<OrderResponse> results = new ArrayList<>(orders.size());
        for (OmsOrder order : orders) {
            results.add(OrderResponse.fromOrder(order));
        }
        return results;
    }

    @Override
    public List<ExecutionResponse> getExecutions(long userId, int limit, int offset) {
        requirePersistence();
        List<ExecutionReport> executions =
                executionRepository.findByUser(userId, boundLimit(limit), Math.max(0, offset));
        List<ExecutionResponse> results = new ArrayList<>(executions.size());
        for (ExecutionReport exec : executions) {
            results.add(ExecutionResponse.fromExecution(exec));
        }
        return results;
    }

    @Override
    public List<PositionResponse> getPositions(long userId) {
        requirePersistence();
        List<PositionResponse> results = new ArrayList<>();
        for (com.openexchange.oms.persistence.PositionAggregate agg
                : executionRepository.aggregatePositionsForUser(userId)) {
            results.add(new PositionResponse(agg.userId(), agg.marketId(), agg.netQuantity()));
        }
        return results;
    }

    private void requirePersistence() {
        if (orderRepository == null || executionRepository == null) {
            throw new IllegalStateException("History unavailable: OMS is running without persistence");
        }
    }

    private static int boundLimit(int limit) {
        if (limit <= 0) return 100;
        return Math.min(limit, 1000);
    }

    @Override
    public Map<String, Object> getBalances(long userId) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("userId", userId);

        List<Map<String, Object>> assetBalances = new ArrayList<>();
        for (Asset asset : Asset.values()) {
            long available = balanceStore.getAvailable(userId, asset.id());
            long locked = balanceStore.getLocked(userId, asset.id());
            if (available > 0 || locked > 0) {
                Map<String, Object> ab = new LinkedHashMap<>();
                ab.put("asset", asset.name());
                ab.put("assetId", asset.id());
                // Exact decimal strings on the wire (oms#39)
                ab.put("available", FixedPoint.format(available));
                ab.put("locked", FixedPoint.format(locked));
                ab.put("total", FixedPoint.format(available + locked));
                assetBalances.add(ab);
            }
        }
        result.put("assets", assetBalances);
        return result;
    }

    @Override
    public List<Map<String, Object>> getMarkets() {
        List<Map<String, Object>> markets = new ArrayList<>();
        for (Market m : Market.ALL) {
            Map<String, Object> mkt = new LinkedHashMap<>();
            mkt.put("marketId", m.marketId());
            mkt.put("symbol", m.symbol());
            mkt.put("baseAsset", m.baseAsset().name());
            mkt.put("quoteAsset", m.quoteAsset().name());
            markets.add(mkt);
        }
        return markets;
    }

    @Override
    public boolean isClusterConnected() {
        return egressAdapter != null && egressAdapter.isConnected();
    }

    @Override
    public int getActiveOrderCount() {
        return coreEngine.getLifecycleManager().getActiveOrderCount();
    }

    @Override
    public void deposit(long userId, int assetId, long amount) {
        balanceStore.deposit(userId, assetId, amount);
    }

    @Override
    public void withdraw(long userId, int assetId, long amount) {
        balanceStore.withdraw(userId, assetId, amount);
    }

    private static com.match.infrastructure.generated.OrderType mapOrderType(OmsOrderType type) {
        return switch (type) {
            case LIMIT -> com.match.infrastructure.generated.OrderType.LIMIT;
            case MARKET -> com.match.infrastructure.generated.OrderType.MARKET;
            case LIMIT_MAKER -> com.match.infrastructure.generated.OrderType.LIMIT_MAKER;
            default -> com.match.infrastructure.generated.OrderType.LIMIT;
        };
    }

    private static com.match.infrastructure.generated.OrderSide mapOrderSide(OrderSide side) {
        return switch (side) {
            case BUY -> com.match.infrastructure.generated.OrderSide.BID;
            case SELL -> com.match.infrastructure.generated.OrderSide.ASK;
        };
    }
}
