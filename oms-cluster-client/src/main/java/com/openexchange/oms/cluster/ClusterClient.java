package com.openexchange.oms.cluster;

import com.match.infrastructure.generated.*;

import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.samples.cluster.ClusterConfig;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Aeron Cluster client for the Order Management System.
 *
 * <p>Adapted from the match-gateway AeronGateway, this client manages the full
 * MediaDriver and AeronCluster connection lifecycle for the OMS. It encodes
 * CreateOrder/CancelOrder SBE messages and sends them to the cluster via ingress,
 * and decodes TradeExecutionBatch, OrderStatusBatch, BookDelta, and BookSnapshot
 * egress messages.</p>
 *
 * <h3>Threading model</h3>
 * <p>Following official Aeron best practices, all cluster operations (pollEgress,
 * sendKeepAlive, offer, queue drain) run on a single polling thread. API threads
 * submit orders through a lock-free MPSC queue which the polling thread drains.</p>
 *
 * <h3>Reconnection</h3>
 * <p>Exponential backoff reconnection with MediaDriver reset after consecutive
 * failures. Stale egress detection forces reconnect when connected but no data
 * flows for an extended period.</p>
 */
public class ClusterClient implements io.aeron.cluster.client.EgressListener, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClusterClient.class);

    // Connection constants
    private static final long HEARTBEAT_INTERVAL_MS = 100;
    private static final long HEARTBEAT_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(HEARTBEAT_INTERVAL_MS);
    private static final long INITIAL_RECONNECT_BACKOFF_MS = 500;
    private static final long MAX_RECONNECT_BACKOFF_MS = 4000;
    private static final int MAX_FAILURES_BEFORE_DRIVER_RESET = 3;
    private static final int MAX_STARTUP_RETRIES = 30;
    private static final long STARTUP_RETRY_DELAY_MS = 2000;
    private static final long STALE_EGRESS_TIMEOUT_MS = 30_000;
    private static final long STATS_LOG_INTERVAL_MS = 10_000;

    // MPSC queue constants
    private static final int ORDER_QUEUE_CAPACITY = 4096;

    // Cluster connection state
    private volatile MediaDriver mediaDriver;
    private volatile AeronCluster cluster;
    private volatile long lastReconnectAttempt = 0;
    private volatile long reconnectBackoffMs = INITIAL_RECONNECT_BACKOFF_MS;
    private volatile int consecutiveReconnectFailures = 0;
    private volatile boolean wasConnected = false;

    // Cluster connection config
    private final String ingressEndpoints;
    private final String egressChannel;

    // SBE encoders for ingress messages (polling thread only)
    private final UnsafeBuffer encodeBuffer = new UnsafeBuffer(new byte[256]);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final CreateOrderEncoder createOrderEncoder = new CreateOrderEncoder();
    private final CancelOrderEncoder cancelOrderEncoder = new CancelOrderEncoder();

    // SBE heartbeat encoder
    private final UnsafeBuffer heartbeatBuffer = new UnsafeBuffer(
            new byte[MessageHeaderEncoder.ENCODED_LENGTH + GatewayHeartbeatEncoder.BLOCK_LENGTH]);
    private final MessageHeaderEncoder heartbeatHeaderEncoder = new MessageHeaderEncoder();
    private final GatewayHeartbeatEncoder heartbeatEncoder = new GatewayHeartbeatEncoder();

    // SBE decoders for egress messages (polling thread only)
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final TradeExecutionBatchDecoder tradeExecutionBatchDecoder = new TradeExecutionBatchDecoder();
    private final OrderStatusBatchDecoder orderStatusBatchDecoder = new OrderStatusBatchDecoder();
    private final BookDeltaDecoder bookDeltaDecoder = new BookDeltaDecoder();
    private final BookSnapshotDecoder bookSnapshotDecoder = new BookSnapshotDecoder();

    // MPSC queue: API threads enqueue OrderSubmission, polling thread drains and encodes/offers
    private final ManyToOneConcurrentArrayQueue<OrderSubmission> orderQueue =
            new ManyToOneConcurrentArrayQueue<>(ORDER_QUEUE_CAPACITY);

    // External listener for decoded egress messages
    private volatile EgressListener egressListener;

    // Shutdown flag for polling loop
    private final AtomicBoolean running = new AtomicBoolean(false);

    // Stats tracking
    private volatile long heartbeatCount = 0;
    private volatile long egressMessageCount = 0;
    private volatile long ordersSent = 0;
    private volatile long lastStatsLogMs = 0;
    private volatile long lastEgressMessageMs = System.currentTimeMillis();

    /**
     * Create a new ClusterClient with connection parameters from environment variables.
     *
     * <p>Environment variables:</p>
     * <ul>
     *   <li>{@code CLUSTER_ADDRESSES} - comma-separated cluster node addresses (default: 127.0.0.1,127.0.0.1,127.0.0.1)</li>
     *   <li>{@code EGRESS_HOST} - host for egress channel (default: 127.0.0.1)</li>
     *   <li>{@code EGRESS_PORT} - port for egress channel (default: 9093)</li>
     * </ul>
     */
    public ClusterClient() {
        final String clusterAddresses = System.getenv().getOrDefault(
                "CLUSTER_ADDRESSES", "127.0.0.1,127.0.0.1,127.0.0.1");
        final String egressHost = System.getenv().getOrDefault("EGRESS_HOST", "127.0.0.1");
        final int egressPort = Integer.parseInt(
                System.getenv().getOrDefault("EGRESS_PORT", "9093"));

        final List<String> hostnames = Arrays.asList(clusterAddresses.split(","));
        this.ingressEndpoints = ClusterConfig.ingressEndpoints(
                hostnames, 9000, ClusterConfig.CLIENT_FACING_PORT_OFFSET);
        this.egressChannel = "aeron:udp?endpoint=" + egressHost + ":" + egressPort;

        log.info("ClusterClient configured: ingress={}, egress={}", ingressEndpoints, egressChannel);
    }

    /**
     * Create a new ClusterClient with explicit connection parameters.
     *
     * @param ingressEndpoints Aeron cluster ingress endpoints string
     * @param egressChannel    Aeron egress channel URI
     */
    public ClusterClient(String ingressEndpoints, String egressChannel) {
        this.ingressEndpoints = ingressEndpoints;
        this.egressChannel = egressChannel;
        log.info("ClusterClient configured: ingress={}, egress={}", ingressEndpoints, egressChannel);
    }

    /**
     * Set the listener for decoded egress messages.
     */
    public void setEgressListener(EgressListener listener) {
        this.egressListener = listener;
    }

    // ==================== Connection Lifecycle ====================

    /**
     * Initialize connection with retries on startup.
     * Blocks until connected or max retries exhausted.
     *
     * @throws RuntimeException if unable to connect after all retries
     */
    public void connect() {
        createMediaDriver();

        for (int attempt = 1; attempt <= MAX_STARTUP_RETRIES; attempt++) {
            try {
                connectToCluster();
                log.info("Connected to cluster on attempt {}", attempt);
                return;
            } catch (Exception e) {
                log.warn("Startup connection attempt {}/{} failed: {}",
                        attempt, MAX_STARTUP_RETRIES, e.getMessage());
                if (attempt == MAX_STARTUP_RETRIES) {
                    throw new RuntimeException(
                            "Failed to connect to cluster after " + MAX_STARTUP_RETRIES + " attempts", e);
                }
                try {
                    Thread.sleep(STARTUP_RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during startup connection", ie);
                }
            }
        }
    }

    /**
     * Create embedded MediaDriver with settings optimized for a cluster client.
     * Uses /dev/shm for pure-memory operations (lower latency than /tmp).
     * Fixed directory name per process prevents stale directory accumulation.
     */
    private void createMediaDriver() {
        if (mediaDriver != null) {
            return;
        }

        cleanStaleMediaDriverDirs();

        String dir = "/dev/shm/aeron-oms-" + ProcessHandle.current().pid();

        mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .aeronDirectoryName(dir)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));

        log.info("MediaDriver created: {}", mediaDriver.aeronDirectoryName());
    }

    /**
     * Clean up stale /dev/shm/aeron-oms-* directories left by previous
     * sessions that did not shut down cleanly. Only removes directories
     * whose PID suffix corresponds to a dead process.
     */
    private void cleanStaleMediaDriverDirs() {
        File shmDir = new File("/dev/shm");
        File[] staleDirs = shmDir.listFiles((dir, name) -> name.startsWith("aeron-oms-"));
        if (staleDirs == null) {
            return;
        }

        long myPid = ProcessHandle.current().pid();
        for (File staleDir : staleDirs) {
            String name = staleDir.getName();
            int lastDash = name.lastIndexOf('-');
            if (lastDash >= 0) {
                try {
                    long dirPid = Long.parseLong(name.substring(lastDash + 1));
                    if (dirPid == myPid) {
                        continue;
                    }
                    if (ProcessHandle.of(dirPid).map(ProcessHandle::isAlive).orElse(false)) {
                        continue;
                    }
                } catch (NumberFormatException e) {
                    // Non-PID suffix, clean it up
                }
            }

            try {
                deleteDirectory(staleDir);
                log.info("Cleaned stale MediaDriver dir: {}", name);
            } catch (Exception e) {
                log.warn("Failed to clean {}: {}", name, e.getMessage());
            }
        }
    }

    private void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }

    /**
     * Connect to the Aeron Cluster. Closes any existing connection first.
     */
    private void connectToCluster() {
        if (cluster != null) {
            CloseHelper.quietClose(cluster);
            cluster = null;
        }

        final AeronCluster.Context clusterCtx = new AeronCluster.Context()
                .egressListener(this)
                .egressChannel(egressChannel)
                .ingressChannel("aeron:udp?term-length=16m|mtu=8k")
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .ingressEndpoints(ingressEndpoints)
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5));

        this.cluster = AeronCluster.connect(clusterCtx);
        log.info("AeronCluster connected: sessionId={}, leader={}, term={}",
                cluster.clusterSessionId(), cluster.leaderMemberId(), cluster.leadershipTermId());

        // Reset backoff and failure tracking on success
        reconnectBackoffMs = INITIAL_RECONNECT_BACKOFF_MS;
        consecutiveReconnectFailures = 0;
        lastEgressMessageMs = System.currentTimeMillis();

        if (!wasConnected) {
            wasConnected = true;
        }

        EgressListener listener = egressListener;
        if (listener != null) {
            try {
                listener.onConnected();
            } catch (Exception e) {
                log.error("EgressListener.onConnected() threw exception", e);
            }
        }
    }

    /**
     * Attempt to reconnect with exponential backoff.
     * Recreates MediaDriver after consecutive failures to clear stale state.
     */
    private void tryReconnect() {
        long now = System.currentTimeMillis();
        if (now - lastReconnectAttempt < reconnectBackoffMs) {
            return;
        }

        lastReconnectAttempt = now;

        // Close old cluster connection
        CloseHelper.quietClose(cluster);
        cluster = null;

        notifyDisconnected();

        // After consecutive failures, recreate MediaDriver to clear stale state
        if (consecutiveReconnectFailures >= MAX_FAILURES_BEFORE_DRIVER_RESET) {
            log.info("Resetting MediaDriver after {} consecutive failures", consecutiveReconnectFailures);
            CloseHelper.quietClose(mediaDriver);
            mediaDriver = null;
            consecutiveReconnectFailures = 0;
        }

        try {
            if (mediaDriver == null) {
                createMediaDriver();
            }

            connectToCluster();
            log.info("Reconnected to cluster successfully");
        } catch (Throwable e) {
            consecutiveReconnectFailures++;
            log.warn("Reconnection failed (attempt {}, next in {}ms): {}: {}",
                    consecutiveReconnectFailures, reconnectBackoffMs,
                    e.getClass().getSimpleName(), e.getMessage());

            // Exponential backoff: 500ms -> 1s -> 2s -> 4s (max)
            reconnectBackoffMs = Math.min(reconnectBackoffMs * 2, MAX_RECONNECT_BACKOFF_MS);

            // Reset MediaDriver on publication/connection issues
            String msg = e.getMessage();
            if (msg != null && (msg.contains("ingressPublication=null") ||
                    msg.contains("AWAIT_PUBLICATION") ||
                    msg.contains("MediaDriver"))) {
                log.info("Publication connection issue detected, will reset MediaDriver");
                CloseHelper.quietClose(mediaDriver);
                mediaDriver = null;
            }
        }
    }

    private void notifyDisconnected() {
        EgressListener listener = egressListener;
        if (listener != null) {
            try {
                listener.onDisconnected();
            } catch (Exception e) {
                log.error("EgressListener.onDisconnected() threw exception", e);
            }
        }
    }

    // ==================== Polling Loop ====================

    /**
     * Main polling loop -- SINGLE THREADED as per Aeron best practices.
     *
     * <p>All operations (pollEgress, heartbeats, order queue drain, offer) happen
     * in this thread. Must be called from a dedicated thread. Runs until
     * {@link #stopPolling()} is called.</p>
     */
    public void startPolling() {
        if (!running.compareAndSet(false, true)) {
            log.warn("Polling already running");
            return;
        }

        final IdleStrategy idleStrategy = new BackoffIdleStrategy(1, 1, 1_000, 100_000);
        long lastHeartbeatNs = System.nanoTime();

        log.info("Starting single-threaded polling loop (Aeron best practice)");

        while (running.get()) {
            final AeronCluster currentCluster = cluster;

            // Check if reconnection needed
            if (currentCluster == null || currentCluster.isClosed()) {
                tryReconnect();
                idleStrategy.idle(0);
                continue;
            }

            try {
                // Poll egress -- handles leader changes internally
                int work = currentCluster.pollEgress();

                // Send SBE heartbeat to cluster to trigger market data flush
                long nowNs = System.nanoTime();
                if (nowNs - lastHeartbeatNs >= HEARTBEAT_INTERVAL_NS) {
                    if (!currentCluster.isClosed()) {
                        sendHeartbeat(currentCluster);
                    }
                    lastHeartbeatNs = nowNs;
                    heartbeatCount++;
                    work++;
                }

                // Drain order queue: encode and offer submissions from API threads
                work += drainOrderQueue(currentCluster);

                // Periodic stats and stale egress detection
                long nowMs = System.currentTimeMillis();
                if (nowMs - lastStatsLogMs > STATS_LOG_INTERVAL_MS) {
                    logStats(currentCluster, nowMs);
                    lastStatsLogMs = nowMs;
                }

                idleStrategy.idle(work);
            } catch (Throwable t) {
                log.error("Polling loop error: {}: {}", t.getClass().getSimpleName(), t.getMessage());
                if (t instanceof Error) {
                    log.error("Fatal error in polling loop", t);
                }
            }
        }

        log.info("Polling loop exited gracefully");
    }

    /**
     * Signal the polling loop to stop.
     */
    public void stopPolling() {
        running.set(false);
    }

    /**
     * Send an SBE-encoded GatewayHeartbeat to the cluster.
     * This triggers the cluster service to flush queued market data.
     */
    private void sendHeartbeat(AeronCluster currentCluster) {
        heartbeatEncoder.wrapAndApplyHeader(heartbeatBuffer, 0, heartbeatHeaderEncoder);
        heartbeatEncoder
                .gatewayId(currentCluster.clusterSessionId())
                .timestamp(System.currentTimeMillis());

        int length = MessageHeaderEncoder.ENCODED_LENGTH + heartbeatEncoder.encodedLength();
        long result = currentCluster.offer(heartbeatBuffer, 0, length);
        if (result < 0 && heartbeatCount % 100 == 0) {
            log.warn("Heartbeat offer failed: {}", offerResultName(result));
        }
    }

    /**
     * Drain the MPSC order queue, encoding each submission and offering to the cluster.
     *
     * @return the number of orders processed
     */
    private int drainOrderQueue(AeronCluster currentCluster) {
        int count = 0;
        OrderSubmission submission;
        while ((submission = orderQueue.poll()) != null) {
            try {
                int length = encodeSubmission(submission);
                long result = currentCluster.offer(encodeBuffer, 0, length);
                if (result < 0) {
                    log.warn("Order offer failed for {}: {}", submission, offerResultName(result));
                } else {
                    ordersSent++;
                }
                count++;
            } catch (Exception e) {
                log.error("Failed to encode/offer order submission: {}", submission, e);
            }
        }
        return count;
    }

    /**
     * Encode an OrderSubmission into the shared encode buffer.
     *
     * @return the total encoded message length (header + body)
     */
    private int encodeSubmission(OrderSubmission submission) {
        if (submission.getType() == OrderSubmission.Type.CREATE) {
            createOrderEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
            createOrderEncoder
                    .userId(submission.getUserId())
                    .price(submission.getPrice())
                    .quantity(submission.getQuantity())
                    .totalPrice(submission.getTotalPrice())
                    .marketId(submission.getMarketId())
                    .orderType(submission.getOrderType())
                    .orderSide(submission.getOrderSide())
                    .omsOrderId(submission.getOmsOrderId());
            return MessageHeaderEncoder.ENCODED_LENGTH + createOrderEncoder.encodedLength();
        } else {
            cancelOrderEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
            cancelOrderEncoder
                    .userId(submission.getUserId())
                    .orderId(submission.getOrderId())
                    .marketId(submission.getMarketId());
            return MessageHeaderEncoder.ENCODED_LENGTH + cancelOrderEncoder.encodedLength();
        }
    }

    private void logStats(AeronCluster currentCluster, long nowMs) {
        long egressAgeMs = nowMs - lastEgressMessageMs;
        var sub = currentCluster.egressSubscription();
        log.info("STATS: egress={}, heartbeats={}, ordersSent={}, connected={}, " +
                        "subConnected={}, images={}, sessionId={}, egressAge={}ms",
                egressMessageCount, heartbeatCount, ordersSent,
                isConnected(), sub.isConnected(), sub.imageCount(),
                currentCluster.clusterSessionId(), egressAgeMs);

        // Stale egress detection: connected but no data flowing
        if (egressAgeMs > STALE_EGRESS_TIMEOUT_MS && egressMessageCount > 0) {
            log.warn("STALE EGRESS DETECTED: no egress messages for {}ms while connected. Forcing reconnect.",
                    egressAgeMs);
            CloseHelper.quietClose(currentCluster);
            cluster = null;
            notifyDisconnected();
            lastReconnectAttempt = 0;
            reconnectBackoffMs = INITIAL_RECONNECT_BACKOFF_MS;
        }
    }

    // ==================== Public API ====================

    /**
     * Check if connected to cluster and ready to accept orders.
     */
    public boolean isConnected() {
        AeronCluster c = cluster;
        return c != null && !c.isClosed();
    }

    /**
     * Submit an order from any thread (thread-safe).
     *
     * <p>The submission is enqueued into a lock-free MPSC queue and will be
     * encoded and offered to the cluster by the polling thread.</p>
     *
     * @param submission the order to submit
     * @return true if enqueued successfully, false if the queue is full
     */
    public boolean submitOrder(OrderSubmission submission) {
        if (submission == null) {
            throw new IllegalArgumentException("submission must not be null");
        }
        boolean enqueued = orderQueue.offer(submission);
        if (!enqueued) {
            log.warn("Order queue full, rejected: {}", submission);
        }
        return enqueued;
    }

    /**
     * Check if the polling loop is currently running.
     */
    public boolean isRunning() {
        return running.get();
    }

    // ==================== EgressListener Implementation ====================

    @Override
    public void onMessage(
            long clusterSessionId,
            long timestamp,
            DirectBuffer buffer,
            int offset,
            int length,
            Header header) {

        egressMessageCount++;
        lastEgressMessageMs = System.currentTimeMillis();

        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return;
        }

        headerDecoder.wrap(buffer, offset);
        int templateId = headerDecoder.templateId();

        if (egressMessageCount <= 5) {
            log.debug("EGRESS[{}]: templateId={}, length={}", egressMessageCount, templateId, length);
        }

        EgressListener listener = egressListener;
        if (listener == null) {
            return;
        }

        switch (templateId) {
            case TradeExecutionBatchDecoder.TEMPLATE_ID:
                dispatchTradeExecutionBatch(buffer, offset, listener);
                break;

            case OrderStatusBatchDecoder.TEMPLATE_ID:
                dispatchOrderStatusBatch(buffer, offset, listener);
                break;

            case BookDeltaDecoder.TEMPLATE_ID:
                bookDeltaDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                listener.onBookDelta(bookDeltaDecoder.marketId(), bookDeltaDecoder);
                break;

            case BookSnapshotDecoder.TEMPLATE_ID:
                bookSnapshotDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                listener.onBookSnapshot(bookSnapshotDecoder.marketId(), bookSnapshotDecoder);
                break;

            default:
                if (egressMessageCount <= 10) {
                    log.debug("EGRESS: Unknown templateId={}", templateId);
                }
                break;
        }
    }

    /**
     * Decode a TradeExecutionBatch and dispatch individual trade callbacks.
     */
    private void dispatchTradeExecutionBatch(DirectBuffer buffer, int offset, EgressListener listener) {
        tradeExecutionBatchDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        int marketId = tradeExecutionBatchDecoder.marketId();

        for (TradeExecutionBatchDecoder.TradesDecoder trade : tradeExecutionBatchDecoder.trades()) {
            boolean takerIsBuy = trade.takerSide() == OrderSide.BID;
            try {
                listener.onTradeExecution(
                        marketId,
                        trade.tradeId(),
                        trade.takerOrderId(),
                        trade.makerOrderId(),
                        trade.takerUserId(),
                        trade.makerUserId(),
                        trade.price(),
                        trade.quantity(),
                        takerIsBuy,
                        trade.takerOmsOrderId(),
                        trade.makerOmsOrderId());
            } catch (Exception e) {
                log.error("EgressListener.onTradeExecution() threw exception for tradeId={}",
                        trade.tradeId(), e);
            }
        }
    }

    /**
     * Decode an OrderStatusBatch and dispatch individual order status callbacks.
     */
    private void dispatchOrderStatusBatch(DirectBuffer buffer, int offset, EgressListener listener) {
        orderStatusBatchDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        int marketId = orderStatusBatchDecoder.marketId();

        for (OrderStatusBatchDecoder.OrdersDecoder order : orderStatusBatchDecoder.orders()) {
            boolean isBuy = order.side() == OrderSide.BID;
            try {
                listener.onOrderStatusUpdate(
                        marketId,
                        order.orderId(),
                        order.userId(),
                        order.statusRaw(),
                        order.price(),
                        order.remainingQty(),
                        order.filledQty(),
                        isBuy,
                        order.omsOrderId());
            } catch (Exception e) {
                log.error("EgressListener.onOrderStatusUpdate() threw exception for orderId={}",
                        order.orderId(), e);
            }
        }
    }

    @Override
    public void onSessionEvent(
            long correlationId,
            long clusterSessionId,
            long leadershipTermId,
            int leaderMemberId,
            EventCode code,
            String detail) {

        if (code == EventCode.OK) {
            log.info("Session connected: leader={}, term={}", leaderMemberId, leadershipTermId);
            return;
        }

        log.warn("Session event: {} - {}", code, detail);

        if (code == EventCode.ERROR || code == EventCode.CLOSED) {
            log.info("Session lost ({}), forcing immediate reconnection", code);
            lastReconnectAttempt = 0;
            reconnectBackoffMs = INITIAL_RECONNECT_BACKOFF_MS;
        }
    }

    @Override
    public void onNewLeader(
            long clusterSessionId,
            long leadershipTermId,
            int leaderMemberId,
            String ingressEndpoints) {
        log.info("New leader elected: member={}, term={}, ingress={}",
                leaderMemberId, leadershipTermId, ingressEndpoints);
    }

    // ==================== Shutdown ====================

    @Override
    public void close() {
        running.set(false);

        CloseHelper.quietClose(cluster);
        CloseHelper.quietClose(mediaDriver);
        cluster = null;
        mediaDriver = null;

        log.info("ClusterClient closed");
    }

    // ==================== Utility ====================

    private static String offerResultName(long result) {
        if (result == Publication.NOT_CONNECTED) return "NOT_CONNECTED";
        if (result == Publication.BACK_PRESSURED) return "BACK_PRESSURED";
        if (result == Publication.ADMIN_ACTION) return "ADMIN_ACTION";
        if (result == Publication.CLOSED) return "CLOSED";
        if (result == Publication.MAX_POSITION_EXCEEDED) return "MAX_POSITION_EXCEEDED";
        return "UNKNOWN(" + result + ")";
    }
}
