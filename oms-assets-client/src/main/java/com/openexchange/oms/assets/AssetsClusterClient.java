// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.assets;

import com.openexchange.assets.infrastructure.generated.BoolFlag;
import com.openexchange.assets.infrastructure.generated.BalanceSnapshotEndDecoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateBatchDecoder;
import com.openexchange.assets.infrastructure.generated.BalanceUpdateDecoder;
import com.openexchange.assets.infrastructure.generated.DepositAckDecoder;
import com.openexchange.assets.infrastructure.generated.DepositEncoder;
import com.openexchange.assets.infrastructure.generated.FeedPositionReportDecoder;
import com.openexchange.assets.infrastructure.generated.HoldAckBatchDecoder;
import com.openexchange.assets.infrastructure.generated.HoldAckDecoder;
import com.openexchange.assets.infrastructure.generated.HoldEncoder;
import com.openexchange.assets.infrastructure.generated.HoldRejectDecoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotBatchDecoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEndDecoder;
import com.openexchange.assets.infrastructure.generated.HoldSnapshotEntryDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderDecoder;
import com.openexchange.assets.infrastructure.generated.MessageHeaderEncoder;
import com.openexchange.assets.infrastructure.generated.QueryFeedPositionEncoder;
import com.openexchange.assets.infrastructure.generated.ReleaseEncoder;
import com.openexchange.assets.infrastructure.generated.RequestBalanceSnapshotEncoder;
import com.openexchange.assets.infrastructure.generated.RequestHoldSnapshotEncoder;
import com.openexchange.assets.infrastructure.generated.SettlementAppliedBatchDecoder;
import com.openexchange.assets.infrastructure.generated.SettlementAppliedDecoder;
import com.openexchange.assets.infrastructure.generated.SubscribeEncoder;
import com.openexchange.assets.infrastructure.generated.WithdrawAckDecoder;
import com.openexchange.assets.infrastructure.generated.WithdrawEncoder;
import com.openexchange.assets.infrastructure.generated.WithdrawRejectDecoder;

import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
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
 *
 * <h3>Balance-feed side-channel (optional)</h3>
 * <p>When {@code balanceFeedChannel} names an Aeron channel, live balances arrive on the AE's
 * conflated side-channel (a plain publication, latest-value per {@code (user, asset)}) instead of
 * riding the cluster session, and this client narrows its session with a {@code Subscribe} to
 * {@code {acks, settlements}} — the firehose of every user's BalanceUpdate stops multiplying per
 * session on the AE egress. Three invariants keep this correct:</p>
 * <ul>
 *   <li><b>Re-declared transport state.</b> The Subscribe narrowing lives on the AE leader and is
 *       deliberately never snapshotted, so it is re-armed on every session establishment AND every
 *       leader change (same principle the assets bridge applies), offered non-blockingly from the
 *       poll loop until accepted — the intent is never dropped and no caller thread ever blocks.
 *       Until it lands the session receives the full firehose, which is the safe direction.</li>
 *   <li><b>Bootstrap order.</b> The feed subscription is opened BEFORE
 *       {@link AssetsEgressListener#onConnected()} fires (which is what enqueues the
 *       balance-snapshot bootstrap), so no live change published after the snapshot's cut can fall
 *       into an unsubscribed gap. The residual last-write-wins window — a feed frame carrying an
 *       older value than the snapshot landing after it — stands only until that key next changes,
 *       and is acceptable because the projection serves reads/pre-checks; the authoritative gate is
 *       the HOLD (see {@link AeronAssetsBalanceStore}'s class doc).</li>
 *   <li><b>The feed is NOT authoritative</b> — the cluster is. Intermediate values may be skipped
 *       (conflation is the feature); the final value always arrives. Snapshot replies and acks stay
 *       on the cluster session (origin-routed, mask-exempt since assets#56).</li>
 * </ul>
 * <p>Feed unset (empty channel) leaves behavior byte-identical to the legacy firehose client:
 * no Subscribe is sent, no subscription opened, nothing polled.</p>
 */
public class AssetsClusterClient
        implements AssetsTransport, io.aeron.cluster.client.EgressListener, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AssetsClusterClient.class);

    // ---- AE cluster port math ----
    // No longer duplicated. It used to be copied here to keep the money client off the
    // heavyweight server module, and that reason still holds; cluster-kit is the small
    // Apache-2.0 jar the engines already share, so the client gets the arithmetic without
    // the server. Copies of a wire contract do not drift loudly: they drift into dialling
    // a port nobody is listening on. By default every AE member listens on the SAME
    // client-facing port and only the host differs.

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

    // ---- Balance-feed side-channel ----
    /**
     * Default Aeron stream id of the AE balance-feed side-channel. Mirrors the AE's
     * {@code InfrastructureConstants.BALANCE_FEED_STREAM_ID} (assets#55) as a LOCAL constant: a
     * stream id is a wire contract like the port math, and the alternative — importing it — would
     * put the heavyweight assets server module on the money client's production classpath.
     */
    public static final int DEFAULT_BALANCE_FEED_STREAM_ID = 4201;
    /**
     * Fragments per feed poll on the shared poll thread. Bounded so a feed burst (leadership-gain
     * full sweep) cannot starve {@code pollEgress} — acks are what callers block on.
     */
    private static final int FEED_POLL_FRAGMENT_LIMIT = 64;

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

    // ---- Balance-feed side-channel state (empty channel = feature OFF, legacy behavior) ----
    private final String balanceFeedChannel;
    private final int balanceFeedStreamId;
    /** Pre-encoded {@code Subscribe{acks, settlements}} frame; immutable after construction. */
    private final UnsafeBuffer subscribeBuffer;
    private final int subscribeLength;
    /** Reassembles fragmented feed frames (a full BalanceUpdateBatch exceeds the default MTU). */
    private final FragmentAssembler feedAssembler;
    /** Lives and dies with the current {@link #cluster}'s owned Aeron client. */
    private volatile Subscription feedSubscription;
    /** Set on session establishment and leader change; cleared when the offer is accepted. */
    private volatile boolean subscribePending;

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

    // money-schema v5 batch egress decoders. A live AE v5 coalesces a drain cycle's same-type events
    // into one frame; these decode the group and replay the SAME per-entry listener callbacks the
    // single-form cases invoke. The single decoders above are kept for back-compat (tools/tests).
    private final HoldAckBatchDecoder holdAckBatchDecoder = new HoldAckBatchDecoder();
    private final BalanceUpdateBatchDecoder balanceUpdateBatchDecoder = new BalanceUpdateBatchDecoder();
    private final SettlementAppliedBatchDecoder settlementAppliedBatchDecoder = new SettlementAppliedBatchDecoder();
    private final HoldSnapshotBatchDecoder holdSnapshotBatchDecoder = new HoldSnapshotBatchDecoder();

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

    // Feed stats (single-writer: the polling thread). egressBalanceUpdateEntries counts balance
    // entries that arrived on the CLUSTER session — with the feed on and the Subscribe accepted,
    // only origin-routed snapshot-reply entries should move it (the firehose negative is testable).
    private volatile long subscribesSent = 0;
    private volatile long feedFramesReceived = 0;
    private volatile long feedEntriesReceived = 0;
    private volatile long egressBalanceUpdateEntries = 0;

    private volatile long leaderTransitionDeadlineMs = 0;
    private volatile long leaderChangeCount = 0;
    private volatile long lastEgressMessageMs = System.currentTimeMillis();
    /** Wall-clock of the last command we offered to the AE. Used to distinguish a real stall (a
     * sent command went unanswered) from a normal idle period (oms#109). 0 = nothing sent yet. */
    private volatile long lastCommandSentMs = 0;

    /**
     * Create a client with connection parameters from environment variables and no balance feed
     * (the legacy full-firehose session).
     *
     * <ul>
     *   <li>{@code AE_CLUSTER_ADDRESSES} - comma-separated AE node hostnames (default: {@code 127.0.0.1})</li>
     *   <li>{@code AE_CLUSTER_PORT_BASE} - base port for the AE cluster (default: {@code 9300})</li>
     *   <li>{@code AE_EGRESS_HOST} - host for the egress channel (default: {@code 127.0.0.1})</li>
     *   <li>{@code AE_EGRESS_PORT} - port for the egress channel (default: {@code 9393})</li>
     * </ul>
     */
    public AssetsClusterClient() {
        this("", DEFAULT_BALANCE_FEED_STREAM_ID);
    }

    /**
     * Create a client with transport parameters from environment variables (see {@link #AssetsClusterClient()})
     * and an explicit balance-feed configuration — the production constructor ({@code OmsConfig}
     * owns the {@code BALANCE_FEED_CHANNEL}/{@code BALANCE_FEED_STREAM_ID} env reads).
     *
     * @param balanceFeedChannel Aeron channel of the AE's conflated balance feed; empty/null = OFF.
     *                           Must be reachable from this client's own embedded driver (UDP
     *                           unicast/multicast or MDC — {@code aeron:ipc} cannot span drivers).
     * @param balanceFeedStreamId stream id of the feed ({@link #DEFAULT_BALANCE_FEED_STREAM_ID})
     */
    public AssetsClusterClient(String balanceFeedChannel, int balanceFeedStreamId) {
        this(envIngressEndpoints(), envEgressChannel(), balanceFeedChannel, balanceFeedStreamId);
    }

    /**
     * Create a client with explicit connection parameters (for tests / non-env deployments), no
     * balance feed.
     *
     * @param ingressEndpoints Aeron cluster ingress endpoints string ({@code 0=host:port,1=host:port,...})
     * @param egressChannel    Aeron egress channel URI
     */
    public AssetsClusterClient(String ingressEndpoints, String egressChannel) {
        this(ingressEndpoints, egressChannel, "", DEFAULT_BALANCE_FEED_STREAM_ID);
    }

    /** Fully explicit constructor; every other constructor funnels here. */
    public AssetsClusterClient(String ingressEndpoints, String egressChannel,
                               String balanceFeedChannel, int balanceFeedStreamId) {
        this.ingressEndpoints = ingressEndpoints;
        this.egressChannel = egressChannel;
        this.aeronDirName = "/dev/shm/aeron-oms-assets-" + ProcessHandle.current().pid();
        this.balanceFeedChannel = balanceFeedChannel == null ? "" : balanceFeedChannel;
        this.balanceFeedStreamId = balanceFeedStreamId;
        if (feedConfigured()) {
            // The Subscribe frame is constant for the life of the client — encode it once here so
            // the poll loop's re-offers never touch an encoder (and the buffer is safely immutable
            // across the connect()-thread / poll-thread handoff).
            this.subscribeBuffer = new UnsafeBuffer(new byte[64]);
            final SubscribeEncoder subscribeEncoder = new SubscribeEncoder();
            subscribeEncoder.wrapAndApplyHeader(subscribeBuffer, 0, new MessageHeaderEncoder())
                    .correlationId(0L)
                    .channels().clear().acks(true).settlements(true);
            this.subscribeLength = MessageHeaderEncoder.ENCODED_LENGTH + subscribeEncoder.encodedLength();
            this.feedAssembler = new FragmentAssembler(this::onFeedFragment);
        } else {
            this.subscribeBuffer = null;
            this.subscribeLength = 0;
            this.feedAssembler = null;
        }
        prefillCommandPool();
        log.info("AssetsClusterClient configured: ingress={}, egress={}, balanceFeed={}",
                ingressEndpoints, egressChannel,
                feedConfigured() ? this.balanceFeedChannel + " stream " + balanceFeedStreamId : "off");
    }

    private static String envIngressEndpoints() {
        final String clusterAddresses = System.getenv().getOrDefault("AE_CLUSTER_ADDRESSES", "127.0.0.1");
        final int portBase = Integer.parseInt(System.getenv().getOrDefault("AE_CLUSTER_PORT_BASE", "9300"));
        return buildIngressEndpoints(Arrays.asList(clusterAddresses.split(",")), portBase);
    }

    private static String envEgressChannel() {
        final String egressHost = System.getenv().getOrDefault("AE_EGRESS_HOST", "127.0.0.1");
        final int egressPort = Integer.parseInt(System.getenv().getOrDefault("AE_EGRESS_PORT", "9393"));
        return "aeron:udp?endpoint=" + egressHost + ":" + egressPort;
    }

    /** True when a balance-feed channel is configured (the Subscribe/side-channel machinery is live). */
    private boolean feedConfigured() {
        return !balanceFeedChannel.isEmpty();
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
            final int clientFacingPort = com.openexchange.cluster.ClusterPorts.port(
                    i, portBase, com.openexchange.cluster.ClusterPorts.CLIENT_FACING_OFFSET);
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
            closeAbandonedCluster(cluster, "pre-connect cleanup");
            cluster = null;
        }

        final AeronCluster.Context clusterCtx = new AeronCluster.Context()
                .egressListener(this)
                .egressChannel(egressChannel)
                // mtu=8k requires loopback or a jumbo-frame path (MTU >= 8192). Drop to 1408 (Aeron
                // default) before pointing at AE nodes off 127.0.0.1 without jumbo frames.
                .ingressChannel("aeron:udp?term-length=4m|mtu=8k")
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

        if (feedConfigured()) {
            // BOOTSTRAP ORDER — this must precede listener.onConnected() below. onConnected() is
            // what enqueues the balance-snapshot bootstrap request, and the feed must already be
            // OPEN and polled when that snapshot streams: any live change published after the
            // snapshot's cut then lands on the feed instead of vanishing into an unsubscribed gap.
            // The remaining honest window is last-write-wins: a conflated feed frame carrying an
            // OLDER value than the snapshot can arrive after it and stand until that (user, asset)
            // next changes. Acceptable by design — the projection is a read/pre-check cache and
            // self-correcting; the authoritative money gate is the HOLD on the cluster session
            // (AeronAssetsBalanceStore's class doc), which a stale read can only false-reject.
            openFeedSubscription();
            // Narrow this brand-new session (it starts CH_ALL on the AE): armed here, offered
            // non-blockingly by the poll loop until accepted. Until then the firehose flows — the
            // safe direction.
            subscribePending = true;
        }

        AssetsEgressListener listener = egressListener;
        if (listener != null) {
            try {
                listener.onConnected();
            } catch (Exception e) {
                log.error("AssetsEgressListener.onConnected() threw exception", e);
            }
        }
    }

    /**
     * (Re)open the balance-feed subscription on the SAME Aeron instance the cluster session uses —
     * the one owned by the current {@link AeronCluster} on this client's embedded driver. Tying its
     * lifecycle to the cluster instance means every teardown path that closes the session (reconnect,
     * stale egress, driver reset, shutdown) reclaims the subscription with it, and every
     * {@code connectToCluster()} re-creates it fresh.
     */
    private void openFeedSubscription() {
        CloseHelper.quietClose(feedSubscription);
        feedSubscription = cluster.context().aeron()
                .addSubscription(balanceFeedChannel, balanceFeedStreamId);
        log.info("AE balance-feed subscription opened: channel={}, stream={}",
                balanceFeedChannel, balanceFeedStreamId);
    }

    /** Attempt to reconnect with exponential backoff; recreate the MediaDriver after repeated failures. */
    private void tryReconnect() {
        long now = System.currentTimeMillis();
        if (now - lastReconnectAttempt < reconnectBackoffMs) {
            return;
        }
        lastReconnectAttempt = now;

        // Close old cluster connection (guaranteed teardown so the abandoned egress
        // subscription's image mappings are released; see closeAbandonedCluster).
        closeFeedSubscription();
        closeAbandonedCluster(cluster, "reconnect");
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

                // Balance-feed side-channel: same single thread, bounded fragment budget so the
                // feed (a leadership-gain sweep can burst) never starves pollEgress above nor the
                // command drain below — and vice versa. No-op (null) when the feed is off.
                final Subscription feedSub = feedSubscription;
                if (feedSub != null) {
                    work += feedSub.poll(feedAssembler, FEED_POLL_FRAGMENT_LIMIT);
                }

                // Re-declare the Subscribe narrowing when armed (fresh session or new leader).
                // Non-blocking single offer per cycle; back-pressure keeps the flag up, so the
                // intent survives until the leader accepts it. Never touches a caller thread.
                if (subscribePending) {
                    work += tryOfferSubscribe(currentCluster);
                }

                long nowNs = System.nanoTime();
                if (nowNs - lastKeepAliveNs >= KEEPALIVE_INTERVAL_NS) {
                    // oms#116: a throwing keep-alive must not skip the drain/watchdog below.
                    try {
                        if (!currentCluster.isClosed()) {
                            boolean sent = currentCluster.sendKeepAlive();
                            if (!sent && keepAliveCount % 10 == 0) {
                                log.warn("AE session keep-alive back-pressured; will retry next interval");
                            }
                        }
                    } catch (RuntimeException e) {
                        if (keepAliveCount % 10 == 0) {
                            log.warn("AE session keep-alive threw ({}); watchdog still runs", e.toString());
                        }
                    }
                    lastKeepAliveNs = nowNs;
                    keepAliveCount++;
                    work++;
                }

                work += drainCommandQueue(currentCluster);

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
     * One non-blocking attempt to land the pre-encoded {@code Subscribe{acks, settlements}} on the
     * current session. Success clears {@link #subscribePending}; ANY failure keeps it set — a dead
     * session is handled by the reconnect machinery, which re-arms the flag on the replacement
     * session anyway, and re-sending a Subscribe twice is idempotent on the AE.
     *
     * @return 1 if the frame was accepted (work done), 0 otherwise
     */
    private int tryOfferSubscribe(AeronCluster currentCluster) {
        final long result = currentCluster.offer(subscribeBuffer, 0, subscribeLength);
        if (result > 0) {
            subscribePending = false;
            subscribesSent++;
            log.info("AE session narrowed to acks+settlements (balances ride the feed side-channel)");
            return 1;
        }
        return 0;
    }

    /** Close the current feed subscription (idempotent, null-safe); reopened by connectToCluster. */
    private void closeFeedSubscription() {
        final Subscription sub = feedSubscription;
        feedSubscription = null;
        CloseHelper.quietClose(sub);
    }

    /**
     * Decode one balance-feed frame and fan out through the SAME listener path as the cluster
     * egress — {@link AssetsEgressListener#onBalanceUpdate} — so the projection and everything
     * downstream cannot tell the transports apart. Runs on the single polling thread (the same
     * thread as {@link #dispatchEgress}), which is why sharing its decoders is safe.
     *
     * <p>Deliberately does NOT touch {@code egressMessageCount}/{@code lastEgressMessageMs}: the
     * stale-egress watchdog reasons about the request-response CLUSTER session, and side-channel
     * traffic must never mask a stalled session. Package-private as a test seam (mirrors
     * {@code dispatchEgress}).</p>
     */
    void onFeedFragment(DirectBuffer buffer, int offset, int length, Header header) {
        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return;
        }
        headerDecoder.wrap(buffer, offset);
        final int templateId = headerDecoder.templateId();

        final AssetsEgressListener l = egressListener;
        if (l == null) {
            return;
        }
        try {
            switch (templateId) {
                case BalanceUpdateDecoder.TEMPLATE_ID:
                    feedFramesReceived++;
                    balanceUpdateDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    l.onBalanceUpdate(balanceUpdateDecoder.userId(), balanceUpdateDecoder.assetId(),
                            balanceUpdateDecoder.available(), balanceUpdateDecoder.locked());
                    feedEntriesReceived++;
                    break;

                case BalanceUpdateBatchDecoder.TEMPLATE_ID:
                    feedFramesReceived++;
                    balanceUpdateBatchDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    for (BalanceUpdateBatchDecoder.UpdatesDecoder e : balanceUpdateBatchDecoder.updates()) {
                        l.onBalanceUpdate(e.userId(), e.assetId(), e.available(), e.locked());
                        feedEntriesReceived++;
                    }
                    break;

                default:
                    // The feed's vocabulary is balance frames only; anything else is a foreign
                    // publication on our stream id — ignore rather than mis-decode.
                    log.warn("AE balance feed: unexpected templateId={} ignored", templateId);
                    break;
            }
        } catch (Exception ex) {
            log.error("AssetsEgressListener feed dispatch threw for templateId={}", templateId, ex);
        }
    }

    /**
     * Drain the MPSC command queue, offering each pre-encoded command to the cluster. Mirrors the ME
     * ClusterClient back-pressure discipline: transient back-pressure retries; NOT_CONNECTED / CLOSED
     * holds the command for the next iteration (reconnect runs at the top of the loop); a command that
     * survives all retries or hits MAX_POSITION_EXCEEDED is dropped so it can't head-of-line block.
     *
     * @return number of commands processed (success + dropped, not held)
     */
    private int drainCommandQueue(AeronCluster currentCluster) {
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
                lastCommandSentMs = System.currentTimeMillis();
                count++;
                continue;
            }

            // Back-pressure or connection error: HOLD the command and return so pollEgress()
            // runs before we retry (mirrors ClusterClient.drainOrderQueue). The old retryOffer
            // spin starved egress polling under back-pressure — money-critical here, since a
            // starved AE egress means a delayed HoldAck / BalanceUpdate.
            if (result == Publication.BACK_PRESSURED || result == Publication.ADMIN_ACTION
                    || result == Publication.NOT_CONNECTED || result == Publication.CLOSED) {
                if (pendingCommand == null) {
                    pendingCommand = cmd;
                    offerHeldCount++;
                }
                return count;
            }

            log.warn("AE command offer failed (dropped): {}", offerResultName(result));
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

    private void logStats(AeronCluster currentCluster, long nowMs) {
        long egressAgeMs = nowMs - lastEgressMessageMs;
        var sub = currentCluster.egressSubscription();
        log.info("AE STATS: egress={}, keepAlives={}, sent={}, connected={}, subConnected={}, "
                        + "images={}, sessionId={}, egressAge={}ms, retried={}, held={}, dropped={}, "
                        + "rejected={}, aeronErrors={}, queueDepth={}, feedFrames={}, feedEntries={}, "
                        + "egressBalanceEntries={}, subscribes={}",
                egressMessageCount, keepAliveCount, commandsSent, isConnected(),
                sub.isConnected(), sub.imageCount(), currentCluster.clusterSessionId(), egressAgeMs,
                offerRetriedCount, offerHeldCount, offerDroppedCount, submitRejectedCount,
                aeronErrorCount.get(), commandQueue.size(), feedFramesReceived, feedEntriesReceived,
                egressBalanceUpdateEntries, subscribesSent);

        // oms#109: the AE is REQUEST-RESPONSE, so egress silence during an idle period (no
        // holds/queries in flight) is normal and must NOT force a reconnect — that churned the
        // session pointlessly. Only a command we SENT since the last egress that has gone
        // unanswered past the timeout is a real stall.
        boolean requestOutstanding = lastCommandSentMs > lastEgressMessageMs;
        if (egressAgeMs > STALE_EGRESS_TIMEOUT_MS && requestOutstanding) {
            log.warn("AE STALE EGRESS: a sent command went unanswered for {}ms while connected. Forcing reconnect.", egressAgeMs);
            final long staleSessionId = currentCluster.clusterSessionId();
            closeFeedSubscription();
            closeAbandonedCluster(currentCluster, "stale-egress forced reconnect");
            cluster = null;
            // Live verification signal: images= in the AE STATS line above must stop climbing
            // once every forced reconnect actually releases the old instance.
            log.info("Old AE AeronCluster instance closed on forced reconnect (sessionId={}); " +
                    "egress subscription and image mappings released", staleSessionId);
            notifyDisconnected();
            lastReconnectAttempt = 0;
            reconnectBackoffMs = INITIAL_RECONNECT_BACKOFF_MS;
        }
    }

    /**
     * Close an abandoned {@link AeronCluster} instance so its egress subscription and mapped
     * image log buffers are guaranteed to be released before a replacement is connected.
     * Mirrors the ME {@code ClusterClient} helper (the two clients are deliberate structural
     * clones on isolated drivers; see class doc).
     *
     * <p>{@code AeronCluster.close()} first sends a session-close request over the ingress
     * publication and only then closes the owned Aeron client, and it is that second step
     * which unmaps the egress image log buffers in /dev/shm. On the reconnect paths the
     * session is typically already broken (stale egress, dead leader, timed-out client
     * conductor), so the first step can throw; a bare {@code CloseHelper.quietClose(cluster)}
     * then swallows the failure and leaks one ~50MB egress image mapping per reconnect cycle.
     * The fallback here closes {@code cluster.context()} directly, which closes the owned
     * Aeron client exactly as the tail of {@code AeronCluster.close()} would have.
     *
     * <p>The embedded MediaDriver is deliberately untouched: cycling the cluster session must
     * not tear down the driver (driver recovery is the separate reset path in
     * {@link #tryReconnect()}).
     *
     * <p>Must only be called from the single polling thread, or before polling starts /
     * during shutdown; AeronCluster lifecycle is single-threaded by design. Idempotence is
     * delegated to {@code AeronCluster.close()} (a no-op once closed); callers null the
     * {@link #cluster} field right after so no path closes the same instance twice.
     */
    private static void closeAbandonedCluster(AeronCluster clusterToClose, String reason) {
        // Context has close() but does not implement AutoCloseable, hence the method reference.
        closeWithFallback(clusterToClose,
                clusterToClose != null ? clusterToClose.context()::close : null, reason);
    }

    /**
     * Close {@code graceful}; if that throws, close {@code fallback} to force resource
     * release. Never throws and is null-safe. Package-private so the close ordering and
     * fallback semantics are unit-testable without a live cluster.
     */
    static void closeWithFallback(AutoCloseable graceful, AutoCloseable fallback, String reason) {
        if (graceful == null) {
            return;
        }
        try {
            graceful.close();
        } catch (Throwable t) {
            log.warn("AE graceful close failed during {} ({}: {}); forcing underlying resource teardown",
                    reason, t.getClass().getSimpleName(), t.getMessage());
            try {
                if (fallback != null) {
                    fallback.close();
                }
            } catch (Throwable ft) {
                log.warn("AE fallback resource teardown also failed during {} ({}: {})",
                        reason, ft.getClass().getSimpleName(), ft.getMessage());
            }
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
    @Override
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

                case HoldAckBatchDecoder.TEMPLATE_ID:
                    holdAckBatchDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    for (HoldAckBatchDecoder.AcksDecoder e : holdAckBatchDecoder.acks()) {
                        l.onHoldAck(e.correlationId(), e.orderId(),
                                e.userId(), e.assetId(), e.amount());
                    }
                    break;

                case HoldRejectDecoder.TEMPLATE_ID:
                    holdRejectDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    l.onHoldReject(holdRejectDecoder.correlationId(), holdRejectDecoder.orderId(),
                            holdRejectDecoder.userId(), holdRejectDecoder.assetId(),
                            holdRejectDecoder.amount(), holdRejectDecoder.reasonRaw());
                    break;

                case BalanceUpdateDecoder.TEMPLATE_ID:
                    egressBalanceUpdateEntries++;
                    balanceUpdateDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    l.onBalanceUpdate(balanceUpdateDecoder.userId(), balanceUpdateDecoder.assetId(),
                            balanceUpdateDecoder.available(), balanceUpdateDecoder.locked());
                    break;

                case BalanceUpdateBatchDecoder.TEMPLATE_ID:
                    balanceUpdateBatchDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    for (BalanceUpdateBatchDecoder.UpdatesDecoder e : balanceUpdateBatchDecoder.updates()) {
                        egressBalanceUpdateEntries++;
                        l.onBalanceUpdate(e.userId(), e.assetId(), e.available(), e.locked());
                    }
                    break;

                case SettlementAppliedDecoder.TEMPLATE_ID:
                    settlementAppliedDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    l.onSettlementApplied(settlementAppliedDecoder.tradeId(),
                            settlementAppliedDecoder.buyerUserId(), settlementAppliedDecoder.sellerUserId());
                    break;

                case SettlementAppliedBatchDecoder.TEMPLATE_ID:
                    settlementAppliedBatchDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    for (SettlementAppliedBatchDecoder.SettlementsDecoder e
                            : settlementAppliedBatchDecoder.settlements()) {
                        l.onSettlementApplied(e.tradeId(), e.buyerUserId(), e.sellerUserId());
                    }
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

                case HoldSnapshotBatchDecoder.TEMPLATE_ID:
                    holdSnapshotBatchDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
                    for (HoldSnapshotBatchDecoder.HoldsDecoder e : holdSnapshotBatchDecoder.holds()) {
                        l.onHoldSnapshotEntry(e.orderId(), e.userId(), e.assetId(), e.remaining());
                    }
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

        if (feedConfigured()) {
            // Leader-local transport state must be re-declared on EVERY leader change (the same
            // principle the assets bridge applies): the Subscribe narrowing lives on the leader and
            // is deliberately never snapshotted, so a session that survives the change falls back
            // to the full firehose (safe direction) until this lands on the new leader. Armed here
            // (this callback runs on the poll thread, inside pollEgress); the poll loop offers it.
            subscribePending = true;
        }

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

    /** Subscribe frames accepted by the AE (>=1 per session/leader generation when the feed is on). */
    public long getSubscribesSent() {
        return subscribesSent;
    }

    /** True while a Subscribe narrowing is armed but not yet accepted by the leader. */
    public boolean isSubscribePending() {
        return subscribePending;
    }

    /** Balance-feed frames decoded (side-channel only; 0 when the feed is off). */
    public long getFeedFramesReceived() {
        return feedFramesReceived;
    }

    /** Balance entries decoded off the feed (singles + batch entries). */
    public long getFeedEntriesReceived() {
        return feedEntriesReceived;
    }

    /**
     * Balance entries (singles + batch entries) that arrived on the CLUSTER session. With the feed
     * on and the Subscribe accepted, only origin-routed snapshot-reply entries move this — a live
     * broadcast landing here would be the firehose leaking past the mask (the integration test's
     * negative assertion).
     */
    public long getEgressBalanceUpdateEntries() {
        return egressBalanceUpdateEntries;
    }

    @Override
    public void close() {
        running.set(false);
        closeFeedSubscription();
        closeAbandonedCluster(cluster, "shutdown");
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

    /**
     * Test seam: copy the pre-encoded Subscribe frame into {@code dst} and return its length, or
     * {@code -1} when no feed is configured (no frame exists). Lets the unit test decode the exact
     * bytes the poll loop offers, without a cluster.
     */
    int copySubscribeForTest(MutableDirectBuffer dst) {
        if (!feedConfigured()) {
            return -1;
        }
        dst.putBytes(0, subscribeBuffer, 0, subscribeLength);
        return subscribeLength;
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
