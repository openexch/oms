// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.app;

import com.openexchange.oms.api.HttpServer;
import com.openexchange.oms.api.OrderService;
import com.openexchange.oms.api.auth.ApiKeyAuthenticationProvider;
import com.openexchange.oms.api.auth.AuthService;
import com.openexchange.oms.api.auth.AuthenticationProvider;
import com.openexchange.oms.api.auth.Authorizer;
import com.openexchange.oms.api.auth.DemoAuthenticationProvider;
import com.openexchange.oms.api.auth.DevAuthenticationProvider;
import com.openexchange.oms.api.auth.GrpcAuthInterceptor;
import com.openexchange.oms.api.auth.JwtAuthenticationProvider;
import com.openexchange.oms.api.auth.RoleBasedAuthorizer;
import com.openexchange.oms.api.audit.AuditLog;
import com.openexchange.oms.api.rest.CorsPolicy;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import com.openexchange.oms.api.grpc.GrpcAccountService;
import com.openexchange.oms.api.grpc.GrpcOrderService;
import com.openexchange.oms.api.grpc.GrpcServer;
import com.openexchange.oms.api.websocket.WebSocketHandler;
import com.openexchange.oms.cluster.ClusterClient;
import com.openexchange.oms.cluster.OrderSubmission;
import com.openexchange.oms.common.domain.Market;
import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.domain.SnowflakeIdGenerator;
import com.openexchange.oms.common.enums.Asset;
import com.openexchange.oms.common.enums.OrderSide;
import com.openexchange.oms.common.enums.OmsOrderStatus;
import com.openexchange.oms.core.OmsCoreEngine;
import com.openexchange.oms.core.OrderLifecycleManager;
import com.openexchange.oms.core.StartupStateRebuilder;
import com.openexchange.oms.core.SyntheticOrderEngine;
import com.openexchange.oms.ledger.BalanceStore;
import com.openexchange.oms.ledger.LedgerService;
import com.openexchange.oms.assets.AeronAssetsBalanceStore;
import com.openexchange.oms.assets.AssetsClusterClient;
import com.openexchange.oms.persistence.PostgresOrderRepository;
import com.openexchange.oms.persistence.PostgresExecutionRepository;
import com.openexchange.oms.persistence.PostgresUserRepository;
import com.openexchange.oms.persistence.PostgresBalanceReadModelStore;
import com.openexchange.oms.persistence.PostgresRiskConfigStore;
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
    private AuditLog auditLog;
    private PrometheusMeterRegistry meterRegistry;
    /** The OMS-side Assets Engine client (E3/E4) — the money authority. */
    private AssetsClusterClient assetsClusterClient;
    /** The Q3 orphan-hold reconciler (E5). */
    private AssetsHoldReconciler assetsHoldReconciler;
    /** The CQRS balance read-model writer (E6); set when PG is present. */
    private PgBalanceReadModelWriter balanceReadModelWriter;

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
        PostgresUserRepository userRepo = null;
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
            userRepo = new PostgresUserRepository(dataSource); // demo accounts (V002__users.sql)
            log.info("PostgreSQL persistence initialized: {}", config.postgresUrl());
        } catch (Exception e) {
            log.warn("PostgreSQL not available — running without persistence: {}", e.getMessage());
        }

        // 2. Balance store: the Assets Engine cluster is the money authority (v0.4).
        // There is no alternative and nothing to configure — money lives on the AE.
        // Its construction is fully ready BEFORE anything below can serve traffic,
        // and every failure is FATAL, exactly like a missing matching-engine cluster:
        // a wrong balance is a wrong dollar, so we refuse to serve rather than degrade.
        log.info("Balance store: Assets Engine cluster (the money authority)");
        AssetsClusterClient assetsClient = new AssetsClusterClient();
        AeronAssetsBalanceStore aeronBalanceStore = new AeronAssetsBalanceStore(
                assetsClient, Asset.values().length, config.aeHoldTimeoutMs(), config.aeAckTimeoutMs());
        // The store's constructor above already called assetsClient.setEgressListener(store),
        // so the listener is attached BEFORE polling starts. Ordering this the other way
        // around (start polling, then construct the store) is a real race, not a theoretical
        // one: the AE cluster is typically local, and the polling loop's first tryReconnect()
        // can complete in well under a millisecond — onConnected() would fire with no
        // listener attached yet, the balance-snapshot bootstrap request would never be sent,
        // and isProjectionReady() would then wait out the full connect timeout for nothing
        // (recovering only on a later leader change, if one ever happens).
        Thread aePollingThread = new Thread(assetsClient::startPolling, "oms-assets-poll");
        aePollingThread.setDaemon(true);
        aePollingThread.start();
        assetsClusterClient = assetsClient; // for graceful shutdown in stop()

        if (!awaitProjectionReady(aeronBalanceStore, config.aeConnectTimeoutMs())) {
            log.error("FATAL: Assets Engine balance projection not ready within {}ms — refusing to "
                            + "serve traffic against a stale/unknown balance view. The AE is the "
                            + "money authority; a healthy AE cluster is required to start.",
                    config.aeConnectTimeoutMs());
            System.exit(1);
            return; // unreachable
        }

        // Boot-time dedupe floor for the local settle side effects (risk onFill, exec
        // persist, applyFill) — the AE itself settles money asynchronously off the ME
        // journal feed, so this floor is what stops the OMS re-applying a trade whose
        // execution row already made it to Postgres before a restart. PG is therefore
        // mandatory: money settlement cannot start unseeded.
        if (executionRepo == null) {
            log.error("FATAL: PostgreSQL is unavailable — the settle dedupe high-water floor "
                    + "(PG max(trade_id)) is money-relevant and cannot start unseeded.");
            System.exit(1);
            return; // unreachable
        }
        aeronBalanceStore.initSettleHighWater(executionRepo.maxTradeId());
        BalanceStore balanceStore = aeronBalanceStore;

        // 3. ID generator
        SnowflakeIdGenerator idGenerator = new SnowflakeIdGenerator(config.nodeId());

        // 4. Ledger service
        LedgerService ledgerService = new LedgerService(balanceStore);
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

        // 6b. Durable risk config: the defaults above are the baseline; stored rows
        // (admin updates + manual circuit-breaker trips) replay on top via the
        // manager's merge path. Memory-only when PG is not configured.
        PostgresRiskConfigStore riskConfigStore = null;
        if (dataSource != null) {
            riskConfigStore = new PostgresRiskConfigStore(dataSource);
            try {
                RiskConfigBootstrap.Result restored =
                        RiskConfigBootstrap.replay(riskConfigStore.loadAll(), configManager, riskEngine);
                log.info("Risk config restored from PG: {} markets loaded, {} manual circuit-breaker "
                        + "trips re-armed", restored.marketsLoaded(), restored.tripsRearmed());
            } catch (Exception e) {
                log.warn("Risk config load failed (running on hardcoded defaults until the next "
                        + "admin update): {}", e.toString());
            }
        } else {
            log.warn("PostgreSQL not configured: risk config is not durable");
        }

        // 7. Core engine (lifecycle + synthetic)
        OrderLifecycleManager lifecycleManager = new OrderLifecycleManager();
        SyntheticOrderEngine syntheticEngine = new SyntheticOrderEngine();
        OmsCoreEngine coreEngine = new OmsCoreEngine(lifecycleManager, syntheticEngine);

        // 7b. Metrics registry (oms#38): Prometheus + JVM binders; exposed at
        // GET /metrics (auth-exempt for the local scraper).
        meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        new JvmMemoryMetrics().bindTo(meterRegistry);
        new JvmGcMetrics().bindTo(meterRegistry);
        new JvmThreadMetrics().bindTo(meterRegistry);
        new ProcessorMetrics().bindTo(meterRegistry);
        new UptimeMetrics().bindTo(meterRegistry);
        io.micrometer.core.instrument.Timer settlementTimer =
                io.micrometer.core.instrument.Timer.builder("oms_settlement_seconds")
                        .description("Trade settlement latency (ledger + risk bookkeeping)")
                        .publishPercentiles(0.5, 0.95, 0.99)
                        .register(meterRegistry);
        io.micrometer.core.instrument.Counter settledTrades =
                io.micrometer.core.instrument.Counter.builder("oms_trades_settled_total")
                        .description("Trades settled (deduplicated)")
                        .register(meterRegistry);

        // 8. Wire settlement handler
        coreEngine.setSettlementHandler((tradeId, buyerUserId, sellerUserId, marketId,
                                          price, quantity, buyerOmsOrderId, sellerOmsOrderId) -> {
            long settleStart = System.nanoTime();
            // settleTradeExecution is idempotent on tradeId; false means the trade was a
            // duplicate (re-delivered on leader switchover) and was NOT newly applied. Skip all
            // side effects so risk positions / open-order counts / overlock are not double-applied.
            boolean applied = ledgerService.settleTradeExecution(tradeId, buyerUserId, sellerUserId,
                    marketId, price, quantity, buyerOmsOrderId, sellerOmsOrderId);
            if (!applied) {
                return false;
            }

            // Update risk engine positions
            riskEngine.onFill(buyerUserId, marketId, OrderSide.BUY, quantity);
            riskEngine.onFill(sellerUserId, marketId, OrderSide.SELL, quantity);

            // Open-order slot release moved to the terminal-status listener (oms#111):
            // FILLED now decrements the slot there like every other terminal. The old
            // fill-path check tested PRE-fill remainingQty, which a full fill never
            // satisfies, so it never fired and the slot leaked +1 per fill (openOrderCounts
            // drifted up at ~the fill rate until users hit the cap → false OPEN_ORDER_LIMIT).
            OmsOrder buyerOrder = lifecycleManager.getOrder(buyerOmsOrderId);

            // Handle overlock for buy orders (per-fill price-improvement hold release; independent
            // of remainingQty so ordering vs applyFill does not matter)
            if (buyerOrder != null && buyerOrder.isBuy()) {
                ledgerService.handleOverlock(buyerOmsOrderId, buyerUserId, marketId,
                        buyerOrder.getPrice(), price, quantity);
            }
            settlementTimer.record(System.nanoTime() - settleStart, java.util.concurrent.TimeUnit.NANOSECONDS);
            settledTrades.increment();
            return true;
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
                if (finalExecutionRepo == null) {
                    return;
                }
                try {
                    finalExecutionRepo.saveExecution(report);
                } catch (Exception e) {
                    // FK race (oms#23): an instant crossing fill reaches here
                    // before the order row's FIRST upsert (order persists ride
                    // later lifecycle events), so the executions FK fails and
                    // the GROUND-TRUTH ledger row was silently lost — the
                    // oms#41 failover E2E measured 232 lost rows in one run.
                    // Upsert the order row, then retry the execution once.
                    Exception failure = e;
                    try {
                        OmsOrder order = lifecycleManager.getOrder(report.getOmsOrderId());
                        if (finalOrderRepo != null && order != null) {
                            finalOrderRepo.saveOrder(order);
                            finalExecutionRepo.saveExecution(report);
                            return;
                        }
                    } catch (Exception retry) {
                        failure = retry;
                    }
                    log.error("Failed to persist execution: tradeId={}", report.getTradeId(), failure);
                }
            }
        });

        // 9b. Rebuild in-memory state from the Postgres ledger (oms#35). Must run
        // BEFORE the cluster client connects so the P1.2 open-orders-snapshot
        // reconciliation trues the restored set up against cluster reality, and
        // before the HTTP server so no user traffic races the rebuild.
        if (orderRepo != null && executionRepo != null) {
            try {
                StartupStateRebuilder.rebuild(
                        orderRepo.findAllOpenOrders(), executionRepo.aggregatePositions(),
                        lifecycleManager, syntheticEngine, riskEngine);
            } catch (Exception e) {
                log.error("State rebuild from Postgres failed — starting with empty state "
                        + "(open orders will be repaired by the cluster snapshot reconcile; "
                        + "positions stay unknown until fills arrive)", e);
            }
        } else {
            log.warn("Postgres unavailable — skipping startup state rebuild (oms#35)");
        }

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

                // Exact since match#30; the CHILD price differs from the admitted
                // parent's, so its notional needs its own overflow guard.
                long totalPrice;
                try {
                    totalPrice = com.match.domain.FixedPoint.multiply(childPrice, parentOrder.getQuantity());
                } catch (ArithmeticException e) {
                    log.error("Triggered child order notional overflows fixed-point — not submitted: "
                            + "parentOmsOrderId={}, childPrice={}, qty={}",
                            parentOrder.getOmsOrderId(), childPrice, parentOrder.getQuantity());
                    return;
                }

                OrderSubmission submission = OrderSubmission.createOrder(
                        parentOrder.getUserId(), parentOrder.getMarketId(),
                        childPrice, parentOrder.getRemainingQty(), totalPrice,
                        sbeType, sbeSide, parentOrder.getOmsOrderId());
                clusterClient.submitOrder(submission);
            }

            @Override
            public boolean submitIcebergSlice(OmsOrder icebergOrder, long sliceQuantity) {
                // Slice notional <= admitted parent notional, so this cannot throw
                // for coherent slices; guarded all the same (exact since match#30).
                long totalPrice;
                try {
                    totalPrice = com.match.domain.FixedPoint.multiply(icebergOrder.getPrice(), sliceQuantity);
                } catch (ArithmeticException e) {
                    log.error("Iceberg slice notional overflows fixed-point — not submitted: "
                            + "omsOrderId={}, price={}, sliceQty={}",
                            icebergOrder.getOmsOrderId(), icebergOrder.getPrice(), sliceQuantity);
                    return false;
                }

                com.match.infrastructure.generated.OrderSide sbeSide =
                        icebergOrder.getSide() == OrderSide.BUY
                                ? com.match.infrastructure.generated.OrderSide.BID
                                : com.match.infrastructure.generated.OrderSide.ASK;

                OrderSubmission submission = OrderSubmission.createOrder(
                        icebergOrder.getUserId(), icebergOrder.getMarketId(),
                        icebergOrder.getPrice(), sliceQuantity, totalPrice,
                        com.match.infrastructure.generated.OrderType.LIMIT,
                        sbeSide, icebergOrder.getOmsOrderId());
                return clusterClient.submitOrder(submission);
            }

            @Override
            public void submitOpenOrdersSnapshotRequest(long requestId) {
                clusterClient.submitOrder(
                        com.openexchange.oms.cluster.OrderSubmission.requestOpenOrdersSnapshot(requestId));
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

        // 11. Shared authorizer (the auth provider itself is built in 12a —
        // demo mode needs the order service for registration funding)
        Authorizer authorizer = new RoleBasedAuthorizer();

        // 11a. Edge policy (oms#37): CORS allowlist + append-only audit log
        CorsPolicy corsPolicy = CorsPolicy.fromSpec(config.corsOrigins());
        auditLog = AuditLog.open(config.auditLogPath());

        // 11b. WebSocket handler
        WebSocketHandler wsHandler = new WebSocketHandler(authorizer);

        // 12. Order service
        OmsOrderServiceImpl orderServiceImpl = new OmsOrderServiceImpl(
                coreEngine, riskEngine, ledgerService, clusterClient,
                balanceStore, egressAdapter, idGenerator, marketDataProvider);
        orderServiceImpl.setMeterRegistry(meterRegistry);
        orderServiceImpl.setRepositories(orderRepo, executionRepo); // history reads (oms#40)
        OrderService orderService = orderServiceImpl;

        // 12a. Auth seam (oms#36): provider per OMS_AUTH_MODE
        AuthService demoAuthService = "demo".equals(config.authMode())
                ? new DemoAuthService(userRepo, orderService)
                : null;
        AuthenticationProvider authProvider = buildAuthProvider(config, demoAuthService);

        // 12b. Operational gauges (oms#38)
        Gauge.builder("oms_active_orders", orderService, OrderService::getActiveOrderCount)
                .description("Orders active in the OMS lifecycle").register(meterRegistry);
        Gauge.builder("oms_ws_connections", wsHandler, WebSocketHandler::getConnectionCount)
                .description("OMS WebSocket connections").register(meterRegistry);
        Gauge.builder("oms_cluster_connected", egressAdapter, a -> a.isConnected() ? 1 : 0)
                .description("Cluster egress session up (1) / down (0)").register(meterRegistry);
        FunctionCounter.builder("oms_egress_status_gaps_total", egressAdapter, OmsEgressAdapter::getStatusGapCount)
                .description("OrderStatus seq gaps detected on the egress wire").register(meterRegistry);
        FunctionCounter.builder("oms_egress_trade_gaps_total", egressAdapter, OmsEgressAdapter::getTradeGapCount)
                .description("TradeExecution id gaps detected on the egress wire").register(meterRegistry);
        FunctionCounter.builder("oms_egress_late_healed_total", egressAdapter, OmsEgressAdapter::getLateHealedCount)
                .description("Trades that arrived out of order and healed their pending hole (benign interleave)")
                .register(meterRegistry);
        Gauge.builder("oms_egress_pending_holes", egressAdapter, OmsEgressAdapter::getPendingHoleCount)
                .description("Open tradeId holes awaiting their late trade").register(meterRegistry);
        FunctionCounter.builder("oms_egress_reorder_total", egressAdapter, OmsEgressAdapter::getEgressReorderCount)
                .description("Layer 2: egress events arriving out of cluster-log order (order key only)").register(meterRegistry);
        Gauge.builder("oms_egress_last_seq", egressAdapter, OmsEgressAdapter::getLastEgressSeq)
                .description("Layer 2: highest egressSeq (cluster-log order key) seen on the egress wire").register(meterRegistry);
        FunctionCounter.builder("oms_reconcile_repaired_total", coreEngine, OmsCoreEngine::getTotalRepairedOrders)
                .description("Orders terminalized by membership repair").register(meterRegistry);
        FunctionCounter.builder("oms_reconcile_relinked_total", coreEngine, OmsCoreEngine::getTotalRelinkedOrders)
                .description("Orders re-linked to cluster ids by membership repair").register(meterRegistry);
        FunctionCounter.builder("oms_ledger_oversettle_total", balanceStore, BalanceStore::getOversettleCount)
                .description("Settlements where locked was under-held and clamped (accounting-invariant break, oms#84)").register(meterRegistry);

        // 12c. AE balance store metrics (E3/E4) — only registered when OMS_BALANCE_STORE=aeron.
        if (aeronBalanceStore != null) {
            final AeronAssetsBalanceStore aeStore = aeronBalanceStore;
            FunctionCounter.builder("oms_assets_hold_timeouts_total", aeStore, AeronAssetsBalanceStore::getHoldTimeouts)
                    .description("AE hold requests that timed out awaiting HoldAck/HoldReject").register(meterRegistry);
            FunctionCounter.builder("oms_assets_amend_orphans_total", aeStore, AeronAssetsBalanceStore::getAmendOrphans)
                    .description("AE amend-delta hold timeouts where the compensator was suppressed (possible transient over-lock until terminal)").register(meterRegistry);
            FunctionCounter.builder("oms_assets_late_acks_total", aeStore, AeronAssetsBalanceStore::getLateAcks)
                    .description("AE acks that arrived after their correlation id was already reaped").register(meterRegistry);
            FunctionCounter.builder("oms_assets_compensators_total", aeStore, AeronAssetsBalanceStore::getCompensatorsSent)
                    .description("AE compensating releases sent for unknown-outcome holds").register(meterRegistry);
            Gauge.builder("oms_assets_settle_high_water", aeStore, AeronAssetsBalanceStore::getSettleHighWater)
                    .description("AE settle-side-effect dedupe high-water tradeId").register(meterRegistry);
            Gauge.builder("oms_assets_projection_ready", aeStore, s -> s.isProjectionReady() ? 1 : 0)
                    .description("AE balance projection ready (1) / not ready (0)").register(meterRegistry);

            // Q3 orphan-hold reconciler (E5): sweep the AE for holds with no live order and release
            // ONLY the provably-never-submitted ones. PG is mandatory for the aeron store (enforced
            // above), so orderRepo is non-null here; still guarded. The initial sweep is armed by the
            // first ME open-orders reconcile (see the post-reconcile hook below).
            final PostgresOrderRepository reconcilerOrderRepo = orderRepo;
            AssetsHoldReconciler.OrderLookup pgLookup =
                    reconcilerOrderRepo != null ? reconcilerOrderRepo::findById : null;
            assetsHoldReconciler = new AssetsHoldReconciler(
                    aeStore, lifecycleManager, pgLookup, java.time.Clock.systemUTC(),
                    coreEngine::isClusterOpenOmsOrderId);
            assetsHoldReconciler.start();
            FunctionCounter.builder("oms_assets_orphan_releases_total", assetsHoldReconciler,
                            AssetsHoldReconciler::getOrphanReleasesTotal)
                    .description("AE holds released as provably-never-submitted orphans (E5)").register(meterRegistry);
            FunctionCounter.builder("oms_assets_reconciler_sweeps_total", assetsHoldReconciler,
                            AssetsHoldReconciler::getSweepsTotal)
                    .description("AE orphan-hold reconciler sweeps run (E5)").register(meterRegistry);
            Gauge.builder("oms_assets_unresolved_orphans", assetsHoldReconciler,
                            AssetsHoldReconciler::getUnresolvedOrphansLastSweep)
                    .description("AE holds not provably releasable at the last sweep (surfaced/pending) (E5)")
                    .register(meterRegistry);
            log.info("Assets orphan-hold reconciler started (initial sweep gated on the first ME open-orders reconcile)");

            // E6: CQRS balance read model — a durable, queryable PG mirror of AE balances for
            // ops/analytics. Fed by the store's change-tap on the poll thread; drained off-thread to
            // the account_balances table. NOT money-authoritative and NEVER read by the money path.
            // Attached here, strictly AFTER awaitProjectionReady() above, so the tap only sees
            // post-bootstrap changes; the full balance set is (re)seeded on the next reconnect
            // snapshot, and every live mutation is captured meanwhile. PG is mandatory for the aeron
            // store (enforced above), so dataSource is non-null here; still guarded.
            if (dataSource != null) {
                balanceReadModelWriter = new PgBalanceReadModelWriter(
                        new PostgresBalanceReadModelStore(dataSource));
                aeStore.setBalanceChangeConsumer(balanceReadModelWriter);
                balanceReadModelWriter.start();
                FunctionCounter.builder("oms_balance_readmodel_rows_written_total", balanceReadModelWriter,
                                PgBalanceReadModelWriter::getRowsWritten)
                        .description("Rows upserted into the CQRS balance read model (E6)").register(meterRegistry);
                FunctionCounter.builder("oms_balance_readmodel_flush_failures_total", balanceReadModelWriter,
                                PgBalanceReadModelWriter::getFlushFailures)
                        .description("Balance read-model flush ticks that failed (PG unavailable etc.) (E6)").register(meterRegistry);
                Gauge.builder("oms_balance_readmodel_dirty", balanceReadModelWriter,
                                PgBalanceReadModelWriter::getDirtyCount)
                        .description("Balance read-model (user,asset) keys awaiting flush (E6)").register(meterRegistry);
                log.info("CQRS balance read-model writer started (mirroring AE balances to account_balances)");
            } else {
                log.warn("PG unavailable — CQRS balance read model disabled (no durable balance mirror)");
            }
        }

        // 13. gRPC services (created before state listener so push methods can be called)
        GrpcOrderService grpcOrderSvc = new GrpcOrderService(orderService, authorizer, auditLog);
        GrpcAccountService grpcAccountSvc = new GrpcAccountService(orderService, authorizer);

        // 13b. Cancel-and-replace ledger hooks (oms#67): resolution installs the amended
        // hold target (releasing any surplus); abort rolls back the incremental hold
        // placed at amend submit. Keeps the lifecycle manager ledger-agnostic.
        lifecycleManager.setReplaceHooks(new OrderLifecycleManager.ReplaceHooks() {
            @Override
            public void onReplaceResolved(com.openexchange.oms.common.domain.OmsOrder order) {
                // OMS-3: single source of truth — the ledger nets already-filled qty into the
                // resolution (release surplus for a shrunk notional, install the amended holdAmount).
                ledgerService.resolveAmendHold(order);
            }

            @Override
            public void onReplaceAborted(com.openexchange.oms.common.domain.OmsOrder order) {
                ledgerService.abortAmendHold(order);
            }
        });

        // 14. Order state listener — pushes to WebSocket + gRPC, handles balance release
        lifecycleManager.setStateListener((order, oldStatus, newStatus) -> {
            com.openexchange.oms.api.dto.OrderResponse resp =
                    com.openexchange.oms.api.dto.OrderResponse.fromOrder(order);
            wsHandler.pushToUser(order.getUserId(), "orders", resp);
            grpcOrderSvc.pushOrderUpdate(order.getUserId(), resp);

            // Release balance hold + open-order slot on any terminal transition
            // out of a state that HELD them (hold is placed before PENDING_NEW,
            // so every state from PENDING_NEW on carries a hold and a slot).
            // PENDING_NEW/PENDING_TRIGGER were missing from the cancel/expiry
            // branch: under election churn an order can still be PENDING_NEW in
            // the OMS (ack lost at the seam) when its cancel confirms, and that
            // PENDING_NEW→CANCELLED transition leaked the slot AND the hold —
            // found by the #10 chaos soak as OPEN_ORDER_LIMIT storms with
            // openOrderCounts ≥500 while only ~37 orders were really open,
            // self-healing only on OMS restart (state rebuild resets counters).
            // Release the balance hold only when the order terminated WITHOUT filling, but
            // release the open-order SLOT on EVERY terminal incl. FILLED (oms#111 — FILLED
            // was leaking a slot per fill because neither this listener nor the fill path
            // decremented it). See OpenOrderSlotPolicy.
            if (OpenOrderSlotPolicy.releasesHold(oldStatus, newStatus)) {
                ledgerService.releaseForCancel(order);
            }
            if (OpenOrderSlotPolicy.releasesSlot(oldStatus, newStatus)) {
                riskEngine.onOrderClosed(order.getUserId());
            }
            // Iceberg terminal cleanup (oms#86): drop the parent from the synthetic
            // engine's refill tracking on ANY terminal (a cancel mid-iceberg used to
            // leave the entry in icebergOrders forever), and on FILLED release the
            // consumed-estimate residual, which is 0 when fills matched the estimate
            // exactly (releaseForCancel is 0-safe) but real money if they did not.
            if (order.getOrderType() == com.openexchange.oms.common.enums.OmsOrderType.ICEBERG
                    && newStatus.isTerminal()) {
                syntheticEngine.removeOrder(order);
                if (newStatus == OmsOrderStatus.FILLED) {
                    ledgerService.releaseForCancel(order);
                }
            }
        });

        // 14b. Slot-count rebaseline after every membership reconcile (oms#49):
        // derived openOrderCounts can drift from lifecycle truth when status
        // transitions are dropped at switchover seams; recomputing from the
        // just-reconciled lifecycle bounds any drift to one snapshot cycle.
        coreEngine.setPostReconcileHook(() -> {
            java.util.HashMap<Long, Long> truth = new java.util.HashMap<>();
            lifecycleManager.forEachActiveOrder(o -> {
                OmsOrderStatus st = o.getStatus();
                if (st == OmsOrderStatus.PENDING_NEW || st == OmsOrderStatus.NEW
                        || st == OmsOrderStatus.PARTIALLY_FILLED || st == OmsOrderStatus.PENDING_TRIGGER) {
                    truth.merge(o.getUserId(), 1L, Long::sum);
                }
            });
            long drift = riskEngine.rebaselineOpenOrderCounts(truth);
            if (drift != 0) {
                log.warn("Open-order slot rebaseline corrected drift of {} across {} users (oms#49)",
                        drift, truth.size());
            }
            // E5: the FIRST completed ME open-orders reconcile is the moment the restored order set
            // has been trued up against cluster reality (ack-lost orders re-linked). Arm the AE
            // orphan-hold reconciler's initial sweep here so it never sweeps against a not-yet-repaired
            // view. Idempotent — later reconciles are no-ops for the reconciler.
            if (assetsHoldReconciler != null) {
                assetsHoldReconciler.onStartupReconcileComplete();
            }
        });

        // 15. Admin service (holds the risk-config persist seam; null store = memory-only)
        OmsAdminServiceImpl adminService = new OmsAdminServiceImpl(configManager, riskEngine, riskConfigStore);
        FunctionCounter.builder("oms_risk_config_persist_failures_total", adminService,
                        OmsAdminServiceImpl::getPersistFailures)
                .description("Admin risk-config/breaker persists that failed (change applied in memory only)")
                .register(meterRegistry);

        // 16. HTTP server
        httpServer = new HttpServer(config.httpPort(), orderService, wsHandler, adminService,
                authProvider, authorizer, corsPolicy, auditLog, meterRegistry);
        httpServer.setAuthService(demoAuthService); // /api/v1/auth/* (demo mode only)
        httpServer.start();

        // 17. gRPC server
        grpcServer = new GrpcServer(config.grpcPort(), grpcOrderSvc, grpcAccountSvc,
                new GrpcAuthInterceptor(authProvider));
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
                    try {
                        egressAdapter.sweepStaleOrphans(); // oms#41: reconcile lost in-flight orders
                    } catch (Exception e) {
                        log.error("Stale-orphan sweep failed", e);
                    }
                },
                1, 1, java.util.concurrent.TimeUnit.SECONDS);
        log.info("GTD expiry checker started (1s interval)");

        log.info("OMS Application started successfully");
    }

    /**
     * Poll {@link AeronAssetsBalanceStore#isProjectionReady()} until it flips true or
     * {@code timeoutMs} elapses. Blocks the caller (the startup thread) so nothing below it in
     * {@link #start()} — HTTP/gRPC servers included — can run until the balance projection is
     * bootstrapped, or the process is about to exit fatally.
     */
    private static boolean awaitProjectionReady(AeronAssetsBalanceStore store, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (store.isProjectionReady()) {
                log.info("AE balance projection ready");
                return true;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while awaiting AE balance projection readiness");
                return false;
            }
        }
        return store.isProjectionReady(); // one last check in case readiness landed at the wire
    }

    private static AuthenticationProvider buildAuthProvider(OmsConfig config, AuthService demoAuthService) {
        switch (config.authMode()) {
            case "dev":
                log.warn("AUTH: dev mode — every request is accepted with caller-chosen identity. "
                        + "NEVER use in production (set OMS_AUTH_MODE=api-key or jwt).");
                return new DevAuthenticationProvider();
            case "demo":
                log.info("AUTH: demo mode — registered users (opaque tokens, self-scoped) plus a "
                        + "dev-token backdoor restricted to userId 1 and the sim range 900000-900999");
                return new DemoAuthenticationProvider(demoAuthService);
            case "jwt":
                log.info("AUTH: jwt mode (HS256)");
                return new JwtAuthenticationProvider(config.jwtSecret());
            case "api-key":
                ApiKeyAuthenticationProvider provider =
                        ApiKeyAuthenticationProvider.parse(config.apiKeys(), config.apiKeysFile());
                if (provider.isEmpty()) {
                    log.warn("AUTH: api-key mode with NO keys configured — every request will be "
                            + "rejected. Set OMS_API_KEYS / OMS_API_KEYS_FILE, or OMS_AUTH_MODE=dev "
                            + "for development.");
                } else {
                    log.info("AUTH: api-key mode");
                }
                return provider;
            default:
                throw new IllegalArgumentException(
                        "Unknown OMS_AUTH_MODE '" + config.authMode() + "' (expected api-key, jwt, demo, or dev)");
        }
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

        if (assetsHoldReconciler != null) {
            assetsHoldReconciler.stop();
        }

        if (assetsClusterClient != null) {
            assetsClusterClient.stopPolling();
            assetsClusterClient.close();
        }

        // E6: after the assets poll thread is stopped (no more change-tap events), drain whatever is
        // still dirty so the mirror is current at graceful shutdown.
        if (balanceReadModelWriter != null) {
            balanceReadModelWriter.stop();
        }

        if (auditLog != null) {
            try {
                auditLog.close();
            } catch (Exception e) {
                log.warn("Audit log close failed: {}", e.getMessage());
            }
        }

        log.info("OMS Application stopped");
    }
}
