package com.openexchange.oms.app;

import com.openexchange.oms.api.HttpServer;
import com.openexchange.oms.api.OrderService;
import com.openexchange.oms.api.AdminService;
import com.openexchange.oms.api.grpc.GrpcAccountService;
import com.openexchange.oms.api.grpc.GrpcOrderService;
import com.openexchange.oms.api.grpc.GrpcServer;
import com.openexchange.oms.api.websocket.WebSocketHandler;
import com.openexchange.oms.cluster.ClusterClient;
import com.openexchange.oms.cluster.OrderSubmission;
import com.openexchange.oms.common.domain.Market;
import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.domain.SnowflakeIdGenerator;
import com.openexchange.oms.common.enums.OrderSide;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.core.OmsCoreEngine;
import com.openexchange.oms.core.OrderLifecycleManager;
import com.openexchange.oms.core.SyntheticOrderEngine;
import com.openexchange.oms.ledger.BalanceStore;
import com.openexchange.oms.ledger.InMemoryBalanceStore;
import com.openexchange.oms.ledger.LedgerService;
import com.openexchange.oms.ledger.RedisBalanceStore;
import io.lettuce.core.RedisClient;
import com.openexchange.oms.persistence.PostgresOrderRepository;
import com.openexchange.oms.persistence.PostgresExecutionRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.openexchange.oms.risk.RiskConfig;
import com.openexchange.oms.risk.RiskConfigManager;
import com.openexchange.oms.risk.RiskEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OMS Application entry point.
 * Wires all components together and manages lifecycle.
 */
public class OmsApplication {

    private static final Logger log = LoggerFactory.getLogger(OmsApplication.class);

    private HttpServer httpServer;
    private GrpcServer grpcServer;
    private ClusterClient clusterClient;

    public static void main(String[] args) {
        OmsApplication app = new OmsApplication();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered");
            app.stop();
        }));

        try {
            app.start();
        } catch (Exception e) {
            log.error("Failed to start OMS application", e);
            System.exit(1);
        }
    }

    public void start() throws Exception {
        log.info("Starting OMS Application...");

        // 1. Load configuration
        OmsConfig config = OmsConfig.loadDefaults();
        log.info("Configuration loaded: httpPort={}, grpcPort={}, nodeId={}",
                config.httpPort(), config.grpcPort(), config.nodeId());

        // 1b. Initialize PostgreSQL connection pool (optional — degrades gracefully)
        HikariDataSource dataSource = null;
        PostgresOrderRepository orderRepo = null;
        PostgresExecutionRepository executionRepo = null;
        try {
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl(config.postgresUrl());
            hikariConfig.setUsername(config.postgresUser());
            hikariConfig.setPassword(config.postgresPassword());
            hikariConfig.setMaximumPoolSize(5);
            hikariConfig.setMinimumIdle(1);
            hikariConfig.setConnectionTimeout(5000);
            dataSource = new HikariDataSource(hikariConfig);
            orderRepo = new PostgresOrderRepository(dataSource);
            executionRepo = new PostgresExecutionRepository(dataSource);
            log.info("PostgreSQL persistence initialized: {}", config.postgresUrl());
        } catch (Exception e) {
            log.warn("PostgreSQL not available — running without persistence: {}", e.getMessage());
        }

        // 2. Create balance store (Redis if available, otherwise in-memory)
        BalanceStore balanceStore;
        try {
            String redisUri = "redis://" + config.redisHost() + ":" + config.redisPort();
            RedisClient redisClient = RedisClient.create(redisUri);
            redisClient.connect().close(); // test connectivity
            balanceStore = new RedisBalanceStore(redisClient);
            log.info("Balance store initialized (Redis: {})", redisUri);
        } catch (Exception e) {
            log.warn("Redis not available — using in-memory balance store: {}", e.getMessage());
            balanceStore = new InMemoryBalanceStore();
        }

        // 3. ID generator
        SnowflakeIdGenerator idGenerator = new SnowflakeIdGenerator(config.nodeId());

        // 4. Ledger service
        LedgerService ledgerService = new LedgerService(balanceStore, idGenerator);
        log.info("Ledger service initialized");

        // 5. Market data provider and balance checker
        OmsMarketDataProvider marketDataProvider = new OmsMarketDataProvider();
        OmsBalanceChecker balanceChecker = new OmsBalanceChecker(balanceStore);

        // 6. Risk engine with default configs
        RiskEngine riskEngine = new RiskEngine(6, marketDataProvider, balanceChecker);
        RiskConfigManager configManager = new RiskConfigManager(riskEngine, 6);

        RiskConfig defaultConfig = RiskConfig.builder()
                .minQuantity(1L)
                .maxQuantity(Long.MAX_VALUE)
                .minNotional(0L)
                .maxNotional(Long.MAX_VALUE)
                .priceCollarPercent(20)
                .circuitBreakerPercent(10)
                .circuitBreakerWindowMs(60_000L)
                .maxOrdersPerSec(100)
                .maxOrdersPerMin(1000)
                .maxOpenOrders(500)
                .maxPositionPerMarket(Long.MAX_VALUE)
                .build();

        for (Market m : Market.ALL) {
            configManager.setConfig(m.marketId(), defaultConfig);
        }
        log.info("Risk engine initialized with {} markets", Market.ALL.length);

        // 7. Core engine (lifecycle + synthetic)
        OrderLifecycleManager lifecycleManager = new OrderLifecycleManager();
        SyntheticOrderEngine syntheticEngine = new SyntheticOrderEngine();
        OmsCoreEngine coreEngine = new OmsCoreEngine(lifecycleManager, syntheticEngine);

        // 8. Wire settlement handler
        coreEngine.setSettlementHandler((tradeId, buyerUserId, sellerUserId, marketId,
                                          price, quantity, buyerOmsOrderId, sellerOmsOrderId) -> {
            ledgerService.settleTradeExecution(tradeId, buyerUserId, sellerUserId,
                    marketId, price, quantity, buyerOmsOrderId, sellerOmsOrderId);

            // Update risk engine positions
            riskEngine.onFill(buyerUserId, marketId, OrderSide.BUY, quantity);
            riskEngine.onFill(sellerUserId, marketId, OrderSide.SELL, quantity);

            // Decrement open order counts for filled orders
            OmsOrder buyerOrder = lifecycleManager.getOrder(buyerOmsOrderId);
            if (buyerOrder != null && buyerOrder.getRemainingQty() == 0) {
                riskEngine.onOrderClosed(buyerUserId);
            }
            OmsOrder sellerOrder = lifecycleManager.getOrder(sellerOmsOrderId);
            if (sellerOrder != null && sellerOrder.getRemainingQty() == 0) {
                riskEngine.onOrderClosed(sellerUserId);
            }

            // Handle overlock for buy orders
            if (buyerOrder != null && buyerOrder.isBuy()) {
                ledgerService.handleOverlock(buyerOmsOrderId, buyerUserId, marketId,
                        buyerOrder.getPrice(), price, quantity);
            }
        });

        // 9. Wire persistence handler
        final PostgresOrderRepository finalOrderRepo = orderRepo;
        final PostgresExecutionRepository finalExecutionRepo = executionRepo;
        coreEngine.setPersistenceHandler(new OmsCoreEngine.PersistenceHandler() {
            @Override
            public void persistOrderUpdate(OmsOrder order) {
                if (finalOrderRepo != null) {
                    try {
                        finalOrderRepo.saveOrder(order);
                    } catch (Exception e) {
                        log.error("Failed to persist order update: omsOrderId={}", order.getOmsOrderId(), e);
                    }
                }
            }

            @Override
            public void persistExecution(com.openexchange.oms.common.domain.ExecutionReport report) {
                if (finalExecutionRepo != null) {
                    try {
                        finalExecutionRepo.saveExecution(report);
                    } catch (Exception e) {
                        log.error("Failed to persist execution: tradeId={}", report.getTradeId(), e);
                    }
                }
            }
        });

        // 10. Cluster client
        clusterClient = new ClusterClient();
        OmsEgressAdapter egressAdapter = new OmsEgressAdapter(coreEngine, marketDataProvider);
        clusterClient.setEgressListener(egressAdapter);

        // Wire cluster submit handler for synthetic triggered orders
        coreEngine.setClusterSubmitHandler(new OmsCoreEngine.ClusterSubmitHandler() {
            @Override
            public void submitTriggeredOrder(OmsOrder parentOrder, com.openexchange.oms.common.enums.OmsOrderType childType, long childPrice) {
                com.match.infrastructure.generated.OrderType sbeType;
                if (childType == com.openexchange.oms.common.enums.OmsOrderType.MARKET) {
                    sbeType = com.match.infrastructure.generated.OrderType.MARKET;
                } else {
                    sbeType = com.match.infrastructure.generated.OrderType.LIMIT;
                }

                com.match.infrastructure.generated.OrderSide sbeSide =
                        parentOrder.getSide() == OrderSide.BUY
                                ? com.match.infrastructure.generated.OrderSide.BID
                                : com.match.infrastructure.generated.OrderSide.ASK;

                long totalPrice = com.match.domain.FixedPoint.multiply(childPrice, parentOrder.getQuantity());

                OrderSubmission submission = OrderSubmission.createOrder(
                        parentOrder.getUserId(), parentOrder.getMarketId(),
                        childPrice, parentOrder.getRemainingQty(), totalPrice,
                        sbeType, sbeSide, parentOrder.getOmsOrderId());
                clusterClient.submitOrder(submission);
            }

            @Override
            public void submitIcebergSlice(OmsOrder icebergOrder, long sliceQuantity) {
                long totalPrice = com.match.domain.FixedPoint.multiply(icebergOrder.getPrice(), sliceQuantity);

                com.match.infrastructure.generated.OrderSide sbeSide =
                        icebergOrder.getSide() == OrderSide.BUY
                                ? com.match.infrastructure.generated.OrderSide.BID
                                : com.match.infrastructure.generated.OrderSide.ASK;

                OrderSubmission submission = OrderSubmission.createOrder(
                        icebergOrder.getUserId(), icebergOrder.getMarketId(),
                        icebergOrder.getPrice(), sliceQuantity, totalPrice,
                        com.match.infrastructure.generated.OrderType.LIMIT,
                        sbeSide, icebergOrder.getOmsOrderId());
                clusterClient.submitOrder(submission);
            }

            @Override
            public void submitCancel(long clusterOrderId, long userId, int marketId) {
                OrderSubmission cancel = OrderSubmission.cancelOrder(
                        userId, clusterOrderId, marketId);
                clusterClient.submitOrder(cancel);
            }
        });

        // Start polling thread (daemon)
        Thread pollingThread = new Thread(clusterClient::startPolling, "oms-cluster-poll");
        pollingThread.setDaemon(true);
        pollingThread.start();

        // Try to connect (non-fatal on failure)
        try {
            clusterClient.connect();
            log.info("Connected to cluster");
        } catch (Exception e) {
            log.warn("Cluster connection failed at startup (will retry in background): {}", e.getMessage());
        }

        // 11. WebSocket handler
        WebSocketHandler wsHandler = new WebSocketHandler();

        // 12. Order service
        OrderService orderService = new OmsOrderServiceImpl(
                coreEngine, riskEngine, ledgerService, clusterClient,
                balanceStore, egressAdapter, idGenerator, marketDataProvider);

        // 13. gRPC services (created before state listener so push methods can be called)
        GrpcOrderService grpcOrderSvc = new GrpcOrderService(orderService);
        GrpcAccountService grpcAccountSvc = new GrpcAccountService(orderService);

        // 14. Order state listener — pushes to WebSocket + gRPC, handles balance release
        lifecycleManager.setStateListener((order, oldStatus, newStatus) -> {
            com.openexchange.oms.api.dto.OrderResponse resp =
                    com.openexchange.oms.api.dto.OrderResponse.fromOrder(order);
            wsHandler.pushToUser(order.getUserId(), "orders", resp);
            grpcOrderSvc.pushOrderUpdate(order.getUserId(), resp);

            // Release balance hold on cluster-confirmed cancellation or expiry
            if ((newStatus == OmsOrderStatus.CANCELLED || newStatus == OmsOrderStatus.EXPIRED)
                    && (oldStatus == OmsOrderStatus.NEW || oldStatus == OmsOrderStatus.PARTIALLY_FILLED)) {
                ledgerService.releaseForCancel(order);
                riskEngine.onOrderClosed(order.getUserId());
            }
            // Release balance hold on cluster rejection (order had a hold placed)
            if (newStatus == OmsOrderStatus.REJECTED
                    && (oldStatus == OmsOrderStatus.NEW || oldStatus == OmsOrderStatus.PARTIALLY_FILLED
                        || oldStatus == OmsOrderStatus.PENDING_NEW)) {
                ledgerService.releaseForCancel(order);
                riskEngine.onOrderClosed(order.getUserId());
            }
        });

        // 15. Admin service
        AdminService adminService = new OmsAdminServiceImpl(configManager, riskEngine);

        // 16. HTTP server
        httpServer = new HttpServer(config.httpPort(), orderService, wsHandler, adminService);
        httpServer.start();

        // 17. gRPC server
        grpcServer = new GrpcServer(config.grpcPort(), grpcOrderSvc, grpcAccountSvc);
        grpcServer.start();
        log.info("gRPC server started on port {}", config.grpcPort());

        // 16. GTD expiry timer (checks every second)
        java.util.concurrent.ScheduledExecutorService gtdScheduler =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "oms-gtd-expiry");
                    t.setDaemon(true);
                    return t;
                });
        gtdScheduler.scheduleAtFixedRate(
                () -> {
                    try {
                        coreEngine.checkGtdExpiry(System.currentTimeMillis());
                    } catch (Exception e) {
                        log.error("GTD expiry check failed", e);
                    }
                },
                1, 1, java.util.concurrent.TimeUnit.SECONDS);
        log.info("GTD expiry checker started (1s interval)");

        log.info("OMS Application started successfully");
    }

    public void stop() {
        log.info("Stopping OMS Application...");

        if (grpcServer != null) {
            grpcServer.stop();
        }

        if (httpServer != null) {
            httpServer.stop();
        }

        if (clusterClient != null) {
            clusterClient.stopPolling();
            clusterClient.close();
        }

        log.info("OMS Application stopped");
    }
}
