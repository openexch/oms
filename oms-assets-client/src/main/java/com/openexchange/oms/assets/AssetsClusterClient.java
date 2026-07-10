// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.BalanceSnapshotEndDecoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateDecoder;
import com.openexchange.assets.infrastructure.generated.DepositAckDecoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.FeedPositionReportDecoder;
import com.openexchange.assets.infrastructure.generated.HoldAckDecoder;
import com.openexchange.assets.infrastructure.generated.HoldEncoder;
import com.openexchange.assets.infrastructure.generated.HoldRejectDecoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEndDecoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEntryDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionEncoder;
import com.openexchange.assets.infrastructure.generated.ReleaseEncoder;
import com.openexchange.assets.infrastructure.generated.RequestBalanceSnapshotEncoder;
import com.openexchange.assets.infrastructure.generated.RequestHoldSnapshotEncoder;
import com.openexchange.assets.infrastructure.generated.SettlementAppliedDecoder;
import com.openexchange.assets.infrastructure.generated.WithdrawAckDecoder;
import com.openexchange.assets.infrastructure.generated.WithdrawEncoder;
import com.openexchange.assets.infrastructure.generated.WithdrawRejectDecoder;

import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ManyToManyConcurrentArrayQueue;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Aeron Cluster client for the Assets Engine (AE) money ledger.
 *
 * <p>Structurally a clone of the matching-engine {@code com.openexchange.oms.cluster.ClusterClient}:
 * an embedded {@link MediaDriver}, a single polling thread that owns all cluster I/O, a lock-free
 * MPSC ingress path, reconnect/backoff with driver reset, and egress dispatch to a listener. Only
 * the wire vocabulary differs: this client speaks the AE <b>money-schema v2</b> (holds, deposits,
 * withdrawals, balance/hold snapshots, feed-position) instead of orders and books.</p>
 *
 * <h3>Why a second, isolated MediaDriver ({@code /dev/shm/aeron-oms-assets-<pid>})</h3>
 * <p>The OMS also runs a matching-engine {@code ClusterClient} on its own driver
 * ({@code /dev/shm/aeron-oms-<pid>}). These are kept as <b>independent failure domains on purpose</b>:
 * the ME client resets <i>its</i> driver after {@link #MAX_FAILURES_BEFORE_DRIVER_RESET} consecutive
 * connect failures (a heavy, disruptive teardown of every publication/subscription on that driver).
 * That reset must never ripple into the money path. Sharing one driver would couple the two — an ME
 * reconnect storm would tear down the AE publications mid-flight. Separate drivers guarantee a hold
 * or settlement in flight is unaffected by anything happening on the ME transport, and vice versa.</p>
 *
 * <h3>Threading model</h3>
 * <p>All cluster operations (pollEgress, sendKeepAlive, offer, queue drain) run on the single
 * {@code oms-assets-poll} thread. API threads submit money commands through a lock-free MPSC queue;
 * each command is SBE-encoded on the calling thread into a pooled buffer (a free-list acquire), then
 * the polling thread drains, offers to the cluster, and returns the buffer to the pool. Egress
 * decoders are touched only on the polling thread.</p>
 *
 * <h3>Reconnection</h3>
 * <p>Exponential backoff with {@link MediaDriver} reset after consecutive failures, plus stale-egress
 * detection. A leader change ({@link #onNewLeader}) fires {@link AssetsEgressListener#onReconnected()}
 * so the store above can re-sync balances/holds across the switchover seam.</p>
 */
public class AssetsClusterClient implements io.aeron.cluster.client.EgressListener, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AssetsClusterClient.class);

    // ---- AE cluster port math (kept in lock-step with the AE's ClusterConfig; see class doc) ----
    // clientFacingPort(nodeId) = portBase + nodeId*PORTS_PER_NODE + CLIENT_FACING_PORT_OFFSET.
    // Duplicated here (not imported from assets-cluster) so the money client depends only on
    // assets-common, never on the heavyweight server module.
    private static final int PORTS_PER_NODE = 100;
    private static final int CLIENT_FACING_PORT_OFFSET = 2;

    // ---- Transport tuning (mirrors the proven ME ClusterClient values) ----
    private static final long KEEPALIVE_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(1_000);
    private static final long INITIAL_RECONNECT_BACKOFF_MS = 500;
    private static final long MAX_RECONNECT_BACKOFF_MS = 4_000;
    private static final int MAX_FAILURES_BEFORE_DRIVER_RESET = 3;
    private static final int MAX_STARTUP_RETRIES = 30;
    private static final long STARTUP_RETRY_DELAY_MS = 2_000;
    private static final long STALE_EGRESS_TIMEOUT_MS = 30_000;
    private static final long STATS_LOG_INTERVAL_MS = 10_000;
    private static final long LEADER_TRANSITION_TIMEOUT_MS = 10_000;

    private static final int SOCKET_BUFFER_LENGTH = 4 * 1024 * 1024;
    private static final int INITIAL_WINDOW_LENGTH = 4 * 1024 * 1024;
    private static final int TERM_BUFFER_LENGTH = 16 * 1024 * 1024;

    // ---- MPSC ingress path ----
    private static final int COMMAND_QUEUE_CAPACITY = 4096;
    /** Buffer bytes per pooled command. Largest money-schema ingress message (Hold) is 44 bytes. */
    private static final int MAX_COMMAND_LENGTH = 128;
    /** Max iterations to retry a back-pressured offer. ~10 ms with the current idle strategy. */
    private static final int OFFER_RETRY_LIMIT = 100;

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
    private final String aeronDirName;

    // ---- Ingress: pooled pre-encoded commands ----
    // A command is either in the free-list pool, in the queue, or held as pendingCommand — never two
    // at once. commandPool is MPMC (many API threads acquire; polling thread + rollback return);
    // commandQueue is MPSC (many API producers; the single polling thread consumes).
    private final ManyToManyConcurrentArrayQueue<PooledCommand> commandPool =
            new ManyToManyConcurrentArrayQueue<>(COMMAND_QUEUE_CAPACITY);
    private final ManyToOneConcurrentArrayQueue<PooledCommand> commandQueue =
            new ManyToOneConcurrentArrayQueue<>(COMMAND_QUEUE_CAPACITY);

    /** Holds a command between polling iterations when offer() hits NOT_CONNECTED / CLOSED. */
    private PooledCommand pendingCommand;

    // SBE encoders are per-thread (API threads encode concurrently, each into its own pooled buffer).
    private final ThreadLocal<Encoders> encoders = ThreadLocal.withInitial(Encoders::new);

    // SBE decoders for egress messages (polling thread only)
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final HoldAckDecoder holdAckDecoder = new HoldAckDecoder();
    private final HoldRejectDecoder holdRejectDecoder = new HoldRejectDecoder();
    private final BalanceUpdateDecoder balanceUpdateDecoder = new BalanceUpdateDecoder();
    private final SettlementAppliedDecoder settlementAppliedDecoder = new SettlementAppliedDecoder();
    private final WithdrawRejectDecoder withdrawRejectDecoder = new WithdrawRejectDecoder();
    private final DepositAckDecoder depositAckDecoder = new DepositAckDecoder();
    private final WithdrawAckDecoder withdrawAckDecoder = new WithdrawAckDecoder();
    private final FeedPositionReportDecoder feedPositionReportDecoder = new FeedPositionReportDecoder();
    private final BalanceSnapshotEndDecoder balanceSnapshotEndDecoder = new BalanceSnapshotEndDecoder();
    private final HoldSnapshotEntryDecoder holdSnapshotEntryDecoder = new HoldSnapshotEntryDecoder();
    private final HoldSnapshotEndDecoder holdSnapshotEndDecoder = new HoldSnapshotEndDecoder();

    // External listener for decoded egress messages
    private volatile AssetsEgressListener egressListener;

    // Shutdown flag for polling loop
    private final AtomicBoolean running = new AtomicBoolean(false);

    // Stats
    private volatile long keepAliveCount = 0;
    private volatile long egressMessageCount = 0;
    private volatile long commandsSent = 0;
    private volatile long lastStatsLogMs = 0;
    private volatile long offerRetriedCount = 0;
    private volatile long offerDroppedCount = 0;
    private volatile long offerHeldCount = 0;
    private volatile long submitRejectedCount = 0;
    private final AtomicLong aeronErrorCount = new AtomicLong();

    private volatile long leaderTransitionDeadlineMs = 0;
    private volatile long leaderChangeCount = 0;
    private volatile long lastEgressMessageMs = System.currentTimeMillis();

    /**
     * Create a client with connection parameters from environment variables.
     *
     * <ul>
     *   <li>{@code AE_CLUSTER_ADDRESSES} - comma-separated AE node hostnames (default: {@code 127.0.0.1})</li>
     *   <li>{@code AE_CLUSTER_PORT_BASE} - base port for the AE cluster (default: {@code 9300})</li>
     *   <li>{@code AE_EGRESS_HOST} - host for the egress channel (default: {@code 127.0.0.1})</li>
     *   <li>{@code AE_EGRESS_PORT} - port for the egress channel (default: {@code 9393})</li>
     * </ul>
     */
    public AssetsClusterClient() {
        final String clusterAddresses = System.getenv().getOrDefault("AE_CLUSTER_ADDRESSES", "127.0.0.1");
        final int portBase = Integer.parseInt(System.getenv().getOrDefault("AE_CLUSTER_PORT_BASE", "9300"));
        final String egressHost = System.getenv().getOrDefault("AE_EGRESS_HOST", "127.0.0.1");
        final int egressPort = Integer.parseInt(System.getenv().getOrDefault("AE_EGRESS_PORT", "9393"));

        final List<String> hostnames = Arrays.asList(clusterAddresses.split(","));
        this.ingressEndpoints = buildIngressEndpoints(hostnames, portBase);
        this.egressChannel = "aeron:udp?endpoint=" + egressHost + ":" + egressPort;
        this.aeronDirName = "/dev/shm/aeron-oms-assets-" + ProcessHandle.current().pid();

        prefillCommandPool();
        log.info("AssetsClusterClient configured: ingress={}, egress={}", ingressEndpoints, egressChannel);
    }

    /**
     * Create a client with explicit connection parameters (for tests / non-env deployments).
     *
     * @param ingressEndpoints Aeron cluster ingress endpoints string ({@code 0=host:port,1=host:port,...})
     * @param egressChannel    Aeron egress channel URI
     */
    public AssetsClusterClient(String ingressEndpoints, String egressChannel) {
        this.ingressEndpoints = ingressEndpoints;
        this.egressChannel = egressChannel;
        this.aeronDirName = "/dev/shm/aeron-oms-assets-" + ProcessHandle.current().pid();
        prefillCommandPool();
        log.info("AssetsClusterClient configured: ingress={}, egress={}", ingressEndpoints, egressChannel);
    }

    private void prefillCommandPool() {
        for (int i = 0; i < COMMAND_QUEUE_CAPACITY; i++) {
            if (!commandPool.offer(new PooledCommand())) {
                break; // pool at capacity; commands in circulation are bounded by what we pre-filled
            }
        }
    }

    /**
     * Build the {@code 0=host:port,1=host:port,...} ingress-endpoints string with the AE's port math.
     * memberId is 0-based and matches the AE's {@code ClusterConfig.ingressEndpoints}.
     */
    private static String buildIngressEndpoints(List<String> hostnames, int portBase) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hostnames.size(); i++) {
            final int clientFacingPort = portBase + (i * PORTS_PER_NODE) + CLIENT_FACING_PORT_OFFSET;
            sb.append(i).append('=').append(hostnames.get(i).trim()).append(':').append(clientFacingPort);
            if (i < hostnames.size() - 1) {
                sb.append(',');
            }
        }
        return sb.toString();
    }

    /** Set the listener for decoded egress messages. */
    public void setEgressListener(AssetsEgressListener listener) {
        this.egressListener = listener;
    }

    // ==================== Connection Lifecycle ====================

    /**
     * Initialize the connection with retries. Blocks until connected or retries exhausted.
     *
     * @throws RuntimeException if unable to connect after all retries
     */
    public void connect() {
        createMediaDriver();

        for (int attempt = 1; attempt <= MAX_STARTUP_RETRIES; attempt++) {
            try {
                connectToCluster();
                log.info("Connected to AE cluster on attempt {}", attempt);
                return;
            } catch (Exception e) {
                log.warn("AE startup connection attempt {}/{} failed: {}",
                        attempt, MAX_STARTUP_RETRIES, e.getMessage());
                if (attempt == MAX_STARTUP_RETRIES) {
                    throw new RuntimeException(
                            "Failed to connect to AE cluster after " + MAX_STARTUP_RETRIES + " attempts", e);
                }
                try {
                    Thread.sleep(STARTUP_RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during AE startup connection", ie);
                }
            }
        }
    }

    /**
     * Create the embedded MediaDriver. Uses a fixed per-process /dev/shm directory, isolated from the
     * ME ClusterClient's driver (see class doc — independent failure domains for the money path).
     */
    private void createMediaDriver() {
        if (mediaDriver != null) {
            return;
        }
        cleanStaleMediaDriverDirs();

        mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .aeronDirectoryName(aeronDirName)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .socketSndbufLength(SOCKET_BUFFER_LENGTH)
                .socketRcvbufLength(SOCKET_BUFFER_LENGTH)
                .initialWindowLength(INITIAL_WINDOW_LENGTH)
                .publicationTermBufferLength(TERM_BUFFER_LENGTH)
                .ipcTermBufferLength(TERM_BUFFER_LENGTH)
                .termBufferSparseFile(false)
                .errorHandler(t -> {
                    aeronErrorCount.incrementAndGet();
                    log.error("AE Aeron driver error #{}", aeronErrorCount.get(), t);
                }));

        log.info("AE MediaDriver created: {}", mediaDriver.aeronDirectoryName());
    }

    /**
     * Clean stale {@code /dev/shm/aeron-oms-assets-*} directories left by dead processes. Only touches
     * this client's own prefix — never the ME client's {@code aeron-oms-*} dirs.
     */
    private void cleanStaleMediaDriverDirs() {
        File shmDir = new File("/dev/shm");
        File[] staleDirs = shmDir.listFiles((dir, name) -> name.startsWith("aeron-oms-assets-"));
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
                    // Non-PID suffix, clean it up.
                }
            }
            try {
                deleteDirectory(staleDir);
                log.info("Cleaned stale AE MediaDriver dir: {}", name);
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

    /** Connect to the AE cluster. Closes any existing connection first. */
    private void connectToCluster() {
        if (cluster != null) {
            CloseHelper.quietClose(cluster);
            cluster = null;
        }

        final AeronCluster.Context clusterCtx = new AeronCluster.Context()
                .egressListener(this)
                .egressChannel(egressChannel)
                // mtu=8k requires loopback or a jumbo-frame path (MTU >= 8192). Drop to 1408 (Aeron
                // default) before pointing at AE nodes off 127.0.0.1 without jumbo frames.
                .ingressChannel("aeron:udp?term-length=16m|mtu=8k")
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .ingressEndpoints(ingressEndpoints)
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(5))
                .errorHandler(t -> {
                    aeronErrorCount.incrementAndGet();
                    log.error("AE AeronCluster error #{}", aeronErrorCount.get(), t);
                });

        this.cluster = AeronCluster.connect(clusterCtx);
        log.info("AE AeronCluster connected: sessionId={}, leader={}, term={}",
                cluster.clusterSessionId(), cluster.leaderMemberId(), cluster.leadershipTermId());

        reconnectBackoffMs = INITIAL_RECONNECT_BACKOFF_MS;
        consecutiveReconnectFailures = 0;
        lastEgressMessageMs = System.currentTimeMillis();
        wasConnected = true;

        AssetsEgressListener listener = egressListener;
        if (listener != null) {
            try {
                listener.onConnected();
            } catch (Exception e) {
                log.error("AssetsEgressListener.onConnected() threw exception", e);
            }
        }
    }

    /** Attempt to reconnect with exponential backoff; recreate the MediaDriver after repeated failures. */
    private void tryReconnect() {
        long now = System.currentTimeMillis();
        if (now - lastReconnectAttempt < reconnectBackoffMs) {
            return;
        }
        lastReconnectAttempt = now;

        CloseHelper.quietClose(cluster);
        cluster = null;
        notifyDisconnected();

        if (consecutiveReconnectFailures >= MAX_FAILURES_BEFORE_DRIVER_RESET) {
            log.info("Resetting AE MediaDriver after {} consecutive failures", consecutiveReconnectFailures);
            CloseHelper.quietClose(mediaDriver);
            mediaDriver = null;
            consecutiveReconnectFailures = 0;
        }

        try {
            if (mediaDriver == null) {
                createMediaDriver();
            }
            connectToCluster();
            log.info("Reconnected to AE cluster successfully");
        } catch (Throwable e) {
            consecutiveReconnectFailures++;
            log.warn("AE reconnection failed (attempt {}, next in {}ms): {}: {}",
                    consecutiveReconnectFailures, reconnectBackoffMs,
                    e.getClass().getSimpleName(), e.getMessage());
            reconnectBackoffMs = Math.min(reconnectBackoffMs * 2, MAX_RECONNECT_BACKOFF_MS);

            String msg = e.getMessage();
            if (msg != null && (msg.contains("ingressPublication=null")
                    || msg.contains("AWAIT_PUBLICATION") || msg.contains("MediaDriver"))) {
                log.info("AE publication connection issue detected, will reset MediaDriver");
                CloseHelper.quietClose(mediaDriver);
                mediaDriver = null;
            }
        }
    }

    private void notifyDisconnected() {
        AssetsEgressListener listener = egressListener;
        if (listener != null) {
            try {
                listener.onDisconnected();
            } catch (Exception e) {
                log.error("AssetsEgressListener.onDisconnected() threw exception", e);
            }
        }
    }

    // ==================== Polling Loop ====================

    /**
     * Main polling loop -- SINGLE THREADED. All cluster I/O happens here. Runs until
     * {@link #stopPolling()}. Must be called from the dedicated {@code oms-assets-poll} thread.
     */
    public void startPolling() {
        if (!running.compareAndSet(false, true)) {
            log.warn("AE polling already running");
            return;
        }

        final IdleStrategy idleStrategy = new BackoffIdleStrategy(1, 1, 1_000, 100_000);
        long lastKeepAliveNs = System.nanoTime();

        log.info("Starting single-threaded AE polling loop");

        while (running.get()) {
            final AeronCluster currentCluster = cluster;

            if (currentCluster == null || currentCluster.isClosed()) {
                tryReconnect();
                idleStrategy.idle(0);
                continue;
            }

            try {
                int work = currentCluster.pollEgress();

                long nowNs = System.nanoTime();
                if (nowNs - lastKeepAliveNs >= KEEPALIVE_INTERVAL_NS) {
                    if (!currentCluster.isClosed()) {
                        boolean sent = currentCluster.sendKeepAlive();
                        if (!sent && keepAliveCount % 10 == 0) {
                            log.warn("AE session keep-alive back-pressured; will retry next interval");
                        }
                    }
                    lastKeepAliveNs = nowNs;
                    keepAliveCount++;
                    work++;
                }

                work += drainCommandQueue(currentCluster, idleStrategy);

                long nowMs = System.currentTimeMillis();
                if (nowMs - lastStatsLogMs > STATS_LOG_INTERVAL_MS) {
                    logStats(currentCluster, nowMs);
                    lastStatsLogMs = nowMs;
                }

                idleStrategy.idle(work);
            } catch (Throwable t) {
                log.error("AE polling loop error: {}: {}", t.getClass().getSimpleName(), t.getMessage());
                if (t instanceof Error) {
                    log.error("Fatal error in AE polling loop", t);
                }
            }
        }

        log.info("AE polling loop exited gracefully");
    }

    /** Signal the polling loop to stop. */
    public void stopPolling() {
        running.set(false);
    }

    /**
     * Drain the MPSC command queue, offering each pre-encoded command to the cluster. Mirrors the ME
     * ClusterClient back-pressure discipline: transient back-pressure retries; NOT_CONNECTED / CLOSED
     * holds the command for the next iteration (reconnect runs at the top of the loop); a command that
     * survives all retries or hits MAX_POSITION_EXCEEDED is dropped so it can't head-of-line block.
     *
     * @return number of commands processed (success + dropped, not held)
     */
    private int drainCommandQueue(AeronCluster currentCluster, IdleStrategy idle) {
        int count = 0;
        while (true) {
            final PooledCommand cmd;
            if (pendingCommand != null) {
                cmd = pendingCommand;
            } else {
                cmd = commandQueue.poll();
                if (cmd == null) {
                    break;
                }
            }

            long result = currentCluster.offer(cmd.buffer, 0, cmd.length);
            if (result >= 0) {
                releaseCommand(cmd);
                commandsSent++;
                count++;
                continue;
            }

            if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION) {
                long retried = retryOffer(currentCluster, cmd, idle);
                if (retried >= 0) {
                    releaseCommand(cmd);
                    commandsSent++;
                    offerRetriedCount++;
                    count++;
                    continue;
                }
                result = retried;
            }

            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED) {
                if (pendingCommand == null) {
                    pendingCommand = cmd;
                    offerHeldCount++;
                }
                return count;
            }

            log.warn("AE command offer failed (template stays queued? no -> dropped): {}",
                    offerResultName(result));
            offerDroppedCount++;
            releaseCommand(cmd);
            count++;
        }
        return count;
    }

    /** Return a command to the free-list pool, clearing the pending slot if it held this command. */
    private void releaseCommand(PooledCommand cmd) {
        if (cmd == pendingCommand) {
            pendingCommand = null;
        }
        commandPool.offer(cmd); // capacity >= commands in circulation, so this always succeeds
    }

    private long retryOffer(AeronCluster currentCluster, PooledCommand cmd, IdleStrategy idle) {
        idle.reset();
        for (int i = 0; i < OFFER_RETRY_LIMIT; i++) {
            long result = currentCluster.offer(cmd.buffer, 0, cmd.length);
            if (result >= 0) {
                return result;
            }
            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED
                    || result == Publication.MAX_POSITION_EXCEEDED) {
                return result;
            }
            idle.idle();
        }
        return Publication.BACK_PRESSURED;
    }

    private void logStats(AeronCluster currentCluster, long nowMs) {
        long egressAgeMs = nowMs - lastEgressMessageMs;
        var sub = currentCluster.egressSubscription();
        log.info("AE STATS: egress={}, keepAlives={}, sent={}, connected={}, subConnected={}, "
                        + "images={}, sessionId={}, egressAge={}ms, retried={}, held={}, dropped={}, "
                        + "rejected={}, aeronErrors={}, queueDepth={}",
                egressMessageCount, keepAliveCount, commandsSent, isConnected(),
                sub.isConnected(), sub.imageCount(), currentCluster.clusterSessionId(), egressAgeMs,
                offerRetriedCount, offerHeldCount, offerDroppedCount, submitRejectedCount,
                aeronErrorCount.get(), commandQueue.size());

        if (egressAgeMs > STALE_EGRESS_TIMEOUT_MS && egressMessageCount > 0) {
            log.warn("AE STALE EGRESS: no egress for {}ms while connected. Forcing reconnect.", egressAgeMs);
            CloseHelper.quietClose(currentCluster);
            cluster = null;
            notifyDisconnected();
            lastReconnectAttempt = 0;
            reconnectBackoffMs = INITIAL_RECONNECT_BACKOFF_MS;
        }
    }

    // ==================== Ingress submit API (thread-safe) ====================

    /** Reserve funds for an order (available -> locked). @return true if queued, false on back-pressure. */
    public boolean submitHold(long correlationId, long orderId, long userId, int assetId, long amount,
                              boolean omsManagedRelease) {
        PooledCommand cmd = acquire();
        if (cmd == null) {
            return false;
        }
        Encoders e = encoders.get();
        e.hold.wrapAndApplyHeader(cmd.buffer, 0, e.header)
                .correlationId(correlationId).orderId(orderId).userId(userId).assetId(assetId).amount(amount)
                .omsManagedRelease(omsManagedRelease ? BoolFlag.TRUE : BoolFlag.FALSE);
        cmd.length = MessageHeaderEncoder.ENCODED_LENGTH + e.hold.encodedLength();
        return enqueue(cmd);
    }

    /** Release a hold's residual (locked -> available). {@code amount < 0} (e.g. -1) releases all remaining. */
    public boolean submitRelease(long orderId, long userId, long amount) {
        PooledCommand cmd = acquire();
        if (cmd == null) {
            return false;
        }
        Encoders e = encoders.get();
        e.release.wrapAndApplyHeader(cmd.buffer, 0, e.header)
                .orderId(orderId).userId(userId).amount(amount);
        cmd.length = MessageHeaderEncoder.ENCODED_LENGTH + e.release.encodedLength();
        return enqueue(cmd);
    }

    /** Credit available balance (external boundary). @return true if queued, false on back-pressure. */
    public boolean submitDeposit(long correlationId, long userId, int assetId, long amount) {
        PooledCommand cmd = acquire();
        if (cmd == null) {
            return false;
        }
        Encoders e = encoders.get();
        e.deposit.wrapAndApplyHeader(cmd.buffer, 0, e.header)
                .correlationId(correlationId).userId(userId).assetId(assetId).amount(amount);
        cmd.length = MessageHeaderEncoder.ENCODED_LENGTH + e.deposit.encodedLength();
        return enqueue(cmd);
    }

    /** Debit available balance (external boundary). @return true if queued, false on back-pressure. */
    public boolean submitWithdraw(long correlationId, long userId, int assetId, long amount) {
        PooledCommand cmd = acquire();
        if (cmd == null) {
            return false;
        }
        Encoders e = encoders.get();
        e.withdraw.wrapAndApplyHeader(cmd.buffer, 0, e.header)
                .correlationId(correlationId).userId(userId).assetId(assetId).amount(amount);
        cmd.length = MessageHeaderEncoder.ENCODED_LENGTH + e.withdraw.encodedLength();
        return enqueue(cmd);
    }

    /** Read-only: ask the AE for its ME-journal feed position (answered by a FeedPositionReport egress). */
    public boolean submitQueryFeedPosition(long correlationId) {
        PooledCommand cmd = acquire();
        if (cmd == null) {
            return false;
        }
        Encoders e = encoders.get();
        e.queryFeed.wrapAndApplyHeader(cmd.buffer, 0, e.header).correlationId(correlationId);
        cmd.length = MessageHeaderEncoder.ENCODED_LENGTH + e.queryFeed.encodedLength();
        return enqueue(cmd);
    }

    /** Read-only: ask the AE to stream every balance (BalanceUpdate*, then BalanceSnapshotEnd). */
    public boolean submitRequestBalanceSnapshot(long correlationId) {
        PooledCommand cmd = acquire();
        if (cmd == null) {
            return false;
        }
        Encoders e = encoders.get();
        e.reqBalSnap.wrapAndApplyHeader(cmd.buffer, 0, e.header).correlationId(correlationId);
        cmd.length = MessageHeaderEncoder.ENCODED_LENGTH + e.reqBalSnap.encodedLength();
        return enqueue(cmd);
    }

    /** Read-only: ask the AE to stream every outstanding hold (HoldSnapshotEntry*, then HoldSnapshotEnd). */
    public boolean submitRequestHoldSnapshot(long correlationId) {
        PooledCommand cmd = acquire();
        if (cmd == null) {
            return false;
        }
        Encoders e = encoders.get();
        e.reqHoldSnap.wrapAndApplyHeader(cmd.buffer, 0, e.header).correlationId(correlationId);
        cmd.length = MessageHeaderEncoder.ENCODED_LENGTH + e.reqHoldSnap.encodedLength();
        return enqueue(cmd);
    }

    /** Acquire a pooled command buffer, or {@code null} when the pool is exhausted (too many in flight). */
    private PooledCommand acquire() {
        PooledCommand cmd = commandPool.poll();
        if (cmd == null) {
            submitRejectedCount++;
            log.warn("AE command pool exhausted, submission rejected (back-pressure)");
        }
        return cmd;
    }

    /** Enqueue an encoded command; on the (should-not-happen) full queue, return it to the pool. */
    private boolean enqueue(PooledCommand cmd) {
        if (!commandQueue.offer(cmd)) {
            commandPool.offer(cmd);
            submitRejectedCount++;
            log.warn("AE command queue full, submission rejected (back-pressure)");
            return false;
        }
        return true;
    }

    // ==================== Egress dispatch (io.aeron EgressListener) ====================

    @Override
    public void onMessage(long clusterSessionId, long timestamp, DirectBuffer buffer,
                          int offset, int length, Header header) {
        dispatchEgress(buffer, offset, length);
    }

    /**
     * Decode one egress message and fan out to the {@link AssetsEgressListener}. Package-private so a
     * unit test can drive the dispatch without booting a live Aeron client (the {@code header} arg of
     * {@link #onMessage} is unused, so this carries all the real logic).
     */
    void dispatchEgress(DirectBuffer buffer, int offset, int length) {
        egressMessageCount++;
        lastEgressMessageMs = System.currentTimeMillis();

        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return;
        }

        headerDecoder.wrap(buffer, offset);
        int templateId = headerDecoder.templateId();

        AssetsEgressListener l = egressListener;
        if (l == null) {
            return;
        }

        try {
            switch (templateId) {
                case HoldAckDecoder.TEMPLATE_ID:
                    holdAckDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    l.onHoldAck(holdAckDecoder.correlationId(), holdAckDecoder.orderId(),
                            holdAckDecoder.userId(), holdAckDecoder.assetId(), holdAckDecoder.amount());
                    break;

                case HoldRejectDecoder.TEMPLATE_ID:
                    holdRejectDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    l.onHoldReject(holdRejectDecoder.correlationId(), holdRejectDecoder.orderId(),
                            holdRejectDecoder.userId(), holdRejectDecoder.assetId(),
                            holdRejectDecoder.amount(), holdRejectDecoder.reasonRaw());
                    break;

                case BalanceUpdateDecoder.TEMPLATE_ID:
                    balanceUpdateDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    l.onBalanceUpdate(balanceUpdateDecoder.userId(), balanceUpdateDecoder.assetId(),
                            balanceUpdateDecoder.available(), balanceUpdateDecoder.locked());
                    break;

                case SettlementAppliedDecoder.TEMPLATE_ID:
                    settlementAppliedDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    l.onSettlementApplied(settlementAppliedDecoder.tradeId(),
                            settlementAppliedDecoder.buyerUserId(), settlementAppliedDecoder.sellerUserId());
                    break;

                case WithdrawRejectDecoder.TEMPLATE_ID:
                    withdrawRejectDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    l.onWithdrawReject(withdrawRejectDecoder.correlationId(), withdrawRejectDecoder.userId(),
                            withdrawRejectDecoder.assetId(), withdrawRejectDecoder.amount(),
                            withdrawRejectDecoder.reasonRaw());
                    break;

                case DepositAckDecoder.TEMPLATE_ID:
                    depositAckDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    l.onDepositAck(depositAckDecoder.correlationId(), depositAckDecoder.userId(),
                            depositAckDecoder.assetId(), depositAckDecoder.amount(),
                            depositAckDecoder.newAvailable());
                    break;

                case WithdrawAckDecoder.TEMPLATE_ID:
                    withdrawAckDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    l.onWithdrawAck(withdrawAckDecoder.correlationId(), withdrawAckDecoder.userId(),
                            withdrawAckDecoder.assetId(), withdrawAckDecoder.amount(),
                            withdrawAckDecoder.newAvailable());
                    break;

                case FeedPositionReportDecoder.TEMPLATE_ID:
                    feedPositionReportDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    l.onFeedPositionReport(feedPositionReportDecoder.correlationId(),
                            feedPositionReportDecoder.consumePosition(),
                            feedPositionReportDecoder.lastAppliedTradeId());
                    break;

                case BalanceSnapshotEndDecoder.TEMPLATE_ID:
                    balanceSnapshotEndDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    l.onBalanceSnapshotEnd(balanceSnapshotEndDecoder.correlationId(),
                            balanceSnapshotEndDecoder.entryCount());
                    break;

                case HoldSnapshotEntryDecoder.TEMPLATE_ID:
                    holdSnapshotEntryDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    l.onHoldSnapshotEntry(holdSnapshotEntryDecoder.orderId(),
                            holdSnapshotEntryDecoder.userId(), holdSnapshotEntryDecoder.assetId(),
                            holdSnapshotEntryDecoder.remaining());
                    break;

                case HoldSnapshotEndDecoder.TEMPLATE_ID:
                    holdSnapshotEndDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    l.onHoldSnapshotEnd(holdSnapshotEndDecoder.correlationId(),
                            holdSnapshotEndDecoder.entryCount());
                    break;

                default:
                    if (egressMessageCount <= 10) {
                        log.debug("AE EGRESS: unknown templateId={}", templateId);
                    }
                    break;
            }
        } catch (Exception ex) {
            log.error("AssetsEgressListener dispatch threw for templateId={}", templateId, ex);
        }
    }

    @Override
    public void onSessionEvent(long correlationId, long clusterSessionId, long leadershipTermId,
                               int leaderMemberId, EventCode code, String detail) {
        if (code == EventCode.OK) {
            log.info("AE session connected: leader={}, term={}", leaderMemberId, leadershipTermId);
            return;
        }
        log.warn("AE session event: {} - {}", code, detail);
        if (code == EventCode.ERROR || code == EventCode.CLOSED) {
            log.info("AE session lost ({}), forcing immediate reconnection", code);
            lastReconnectAttempt = 0;
            reconnectBackoffMs = INITIAL_RECONNECT_BACKOFF_MS;
        }
    }

    @Override
    public void onNewLeader(long clusterSessionId, long leadershipTermId, int leaderMemberId,
                            String ingressEndpoints) {
        // AeronCluster redirects the ingress publication to the new leader before this fires; brief
        // NOT_CONNECTED windows on offer() are absorbed by the pendingCommand slot in drainCommandQueue.
        leaderTransitionDeadlineMs = System.currentTimeMillis() + LEADER_TRANSITION_TIMEOUT_MS;
        leaderChangeCount++;
        log.info("AE new leader: member={}, term={}, ingress={} (transition window {}ms)",
                leaderMemberId, leadershipTermId, ingressEndpoints, LEADER_TRANSITION_TIMEOUT_MS);

        // Tell the store above to re-sync money state lost at the switchover seam.
        AssetsEgressListener listener = egressListener;
        if (listener != null) {
            try {
                listener.onReconnected();
            } catch (Exception e) {
                log.error("AssetsEgressListener.onReconnected() threw exception", e);
            }
        }
    }

    // ==================== Accessors / Shutdown ====================

    /** True if connected to the AE cluster and ready to accept commands. */
    public boolean isConnected() {
        AeronCluster c = cluster;
        return c != null && !c.isClosed();
    }

    public boolean isRunning() {
        return running.get();
    }

    public boolean inLeaderTransition() {
        return System.currentTimeMillis() < leaderTransitionDeadlineMs;
    }

    public long getLeaderChangeCount() {
        return leaderChangeCount;
    }

    public long getEgressMessageCount() {
        return egressMessageCount;
    }

    public long getCommandsSent() {
        return commandsSent;
    }

    public long getSubmitRejectedCount() {
        return submitRejectedCount;
    }

    public long getOfferRetriedCount() {
        return offerRetriedCount;
    }

    public long getOfferDroppedCount() {
        return offerDroppedCount;
    }

    public long getOfferHeldCount() {
        return offerHeldCount;
    }

    public long getAeronErrorCount() {
        return aeronErrorCount.get();
    }

    @Override
    public void close() {
        running.set(false);
        CloseHelper.quietClose(cluster);
        CloseHelper.quietClose(mediaDriver);
        cluster = null;
        mediaDriver = null;
        log.info("AssetsClusterClient closed");
    }

    // ==================== Test seams ====================

    /**
     * Test seam: drain one queued command's encoded bytes into {@code dst} and return the encoded
     * length (or {@code -1} if the queue is empty). Returns the command to the pool. Lets the
     * encode-side unit test round-trip a submitted message through the real decoders without a cluster.
     */
    int copyNextQueuedForTest(MutableDirectBuffer dst) {
        PooledCommand cmd = commandQueue.poll();
        if (cmd == null) {
            return -1;
        }
        int len = cmd.length;
        dst.putBytes(0, cmd.buffer, 0, len);
        commandPool.offer(cmd);
        return len;
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

    /**
     * A reusable, pre-encoded ingress command. Owned exclusively by one thread while it moves
     * pool -> queue -> (offer) -> pool, so its buffer needs no synchronization.
     */
    static final class PooledCommand {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_COMMAND_LENGTH]);
        int length;
    }

    /** Per-thread bundle of SBE ingress encoders (API threads encode concurrently, each into its own buffer). */
    private static final class Encoders {
        final MessageHeaderEncoder header = new MessageHeaderEncoder();
        final HoldEncoder hold = new HoldEncoder();
        final ReleaseEncoder release = new ReleaseEncoder();
        final DepositEncoder deposit = new DepositEncoder();
        final WithdrawEncoder withdraw = new WithdrawEncoder();
        final QueryFeedPositionEncoder queryFeed = new QueryFeedPositionEncoder();
        final RequestBalanceSnapshotEncoder reqBalSnap = new RequestBalanceSnapshotEncoder();
        final RequestHoldSnapshotEncoder reqHoldSnap = new RequestHoldSnapshotEncoder();
    }
}
