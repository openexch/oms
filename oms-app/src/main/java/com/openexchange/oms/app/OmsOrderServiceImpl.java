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
import org.checkerframework.checker.nullness.qual.NonNull;
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

    // Slippage multiplier for market buy hold estimation (5% above best ask)
    private static final double MARKET_BUY_SLIPPAGE = 1.05;

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
            order.setPrice(FixedPoint.fromDouble(request.getPrice()));
            order.setQuantity(FixedPoint.fromDouble(request.getQuantity()));
            order.setRemainingQty(order.getQuantity());
            order.setStopPrice(FixedPoint.fromDouble(request.getStopPrice()));
            order.setTrailingDelta(FixedPoint.fromDouble(request.getTrailingDelta()));
            order.setDisplayQuantity(FixedPoint.fromDouble(request.getDisplayQuantity()));
            order.setExpiresAtMs(request.getExpiresAtMs());
            order.setClientOrderId(request.getClientOrderId());

            if (orderType == OmsOrderType.ICEBERG) {
                order.setHiddenQuantity(order.getQuantity());
            }

            // 1. Register with lifecycle manager → PENDING_RISK
            OrderLifecycleManager lcm = coreEngine.getLifecycleManager();
            lcm.registerOrder(order);

            // 2. Risk check
            RiskResult riskResult = riskEngine.check(
                    order.getUserId(), order.getMarketId(), side, orderType,
                    order.getPrice(), order.getQuantity());

            if (!riskResult.isPassed()) {
                lcm.onRiskRejected(order.getOmsOrderId(), riskResult.getRejectReason());
                return CreateOrderResponse.rejected(riskResult.getRejectReason());
            }

            // 3. Risk passed → PENDING_HOLD
            lcm.onRiskPassed(order.getOmsOrderId());

            // 4. Place ledger hold
            List<LedgerEntry> holdEntries = ledgerService.holdForOrder(order);
            if (holdEntries.isEmpty()) {
                lcm.onHoldFailed(order.getOmsOrderId(), "Insufficient balance");
                return CreateOrderResponse.rejected("Insufficient balance");
            }

            // 5. Hold placed → PENDING_NEW
            lcm.onHoldPlaced(order.getOmsOrderId());

            // 6. Synthetic orders go to PENDING_TRIGGER
            if (orderType.isSynthetic()) {
                lcm.onPendingTrigger(order.getOmsOrderId());
                coreEngine.getSyntheticEngine().registerOrder(order);
                return CreateOrderResponse.accepted(order.getOmsOrderId(), order.getStatus().name());
            }

            // 6b. For market buy orders, estimate price from best ask for hold calculation
            if (orderType == OmsOrderType.MARKET && side == OrderSide.BUY) {
                long bestAsk = marketDataProvider.getBestAsk(order.getMarketId());
                if (bestAsk <= 0) {
                    lcm.onHoldFailed(order.getOmsOrderId(), "No liquidity (no best ask)");
                    return CreateOrderResponse.rejected("No liquidity available for market buy");
                }
                // Use best ask with slippage buffer as the estimated price for holding
                long estimatedPrice = (long) (bestAsk * MARKET_BUY_SLIPPAGE);
                order.setPrice(estimatedPrice);
            }

            // 7. Build and submit to cluster
            OrderSubmission submission = getOrderSubmission(orderType, side, order);

            boolean enqueued = clusterClient.submitOrder(submission);
            if (!enqueued) {
                // Queue full — release hold and reject
                ledgerService.releaseForCancel(order);
                lcm.onHoldFailed(order.getOmsOrderId(), "Order queue full");
                return CreateOrderResponse.rejected("Order queue full");
            }

            // 8. Track open order count
            riskEngine.onOrderOpened(order.getUserId());

            return CreateOrderResponse.accepted(order.getOmsOrderId(), order.getStatus().name());

        } catch (IllegalArgumentException e) {
            return CreateOrderResponse.rejected(e.getMessage());
        }
    }

    private static @NonNull OrderSubmission getOrderSubmission(OmsOrderType orderType, OrderSide side, OmsOrder order) {
        com.match.infrastructure.generated.OrderType sbeOrderType = mapOrderType(orderType);
        com.match.infrastructure.generated.OrderSide sbeOrderSide = mapOrderSide(side);
        long totalPrice = FixedPoint.multiply(order.getPrice(), order.getQuantity());

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

        // If order is PENDING_TRIGGER (synthetic), remove from synthetic engine and release hold
        if (order.getStatus() == OmsOrderStatus.PENDING_TRIGGER) {
            coreEngine.getSyntheticEngine().removeOrder(order);
            ledgerService.releaseForCancel(order);
            lcm.onClusterOrderStatus(omsOrderId, 0, 3, 0, 0); // status 3 = CANCELLED
            riskEngine.onOrderClosed(order.getUserId());
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
    public OrderResponse getOrder(long omsOrderId) {
        OmsOrder order = coreEngine.getLifecycleManager().getOrder(omsOrderId);
        if (order == null) return null;
        return OrderResponse.fromOrder(order);
    }

    @Override
    public List<OrderResponse> queryOrders(long userId, String status) {
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
                ab.put("available", FixedPoint.toDouble(available));
                ab.put("locked", FixedPoint.toDouble(locked));
                ab.put("total", FixedPoint.toDouble(available + locked));
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
