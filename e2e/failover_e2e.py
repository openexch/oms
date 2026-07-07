#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""
Full-stack failover E2E (oms#41 / P5.1) — P1's acceptance test in miniature,
runnable on a clean clone (GitHub Actions) or a dev box.

What it does:
  1. Resets a dedicated Postgres database (never `oms`) and, if needed,
     spawns a private Redis.
  2. Launches a REAL 3-node Aeron cluster (embedded media drivers, isolated
     aeron dirs + port base so a live dev cluster on the same box is safe)
     and a REAL OMS wired to it.
  3. Seeds two users and streams crossing limit orders.
  4. kill -9's the CURRENT LEADER mid-load, keeps submitting through the
     election, waits for the new leader + OMS reconnect, then restarts the
     killed node and waits for it to rejoin.
  5. Drains to quiescence (cancels the resting tail) and asserts:
       A. every order in history is terminal — nothing stuck open
       B. per-order filledQty == SUM(executions.quantity)   [PG]
       C. every tradeId has exactly one BUY + one SELL row, equal price/qty
       D. balances == seed ± exact execution deltas, locked == 0 (holds all
          released), REST strings vs integer fixed-point math
       E. /positions == net of executions (dogfoods the oms#40 API)
       F. the failover was real: leader changed, orders accepted after kill

Stdlib only. Exit 0 = pass. Logs land in <workdir>/logs; on failure the
tails are printed.

Env (all optional on CI; defaults are safe next to a live dev stack):
  E2E_MATCH_JAR   path to match-cluster.jar   (default ../match/... layout)
  E2E_OMS_JAR     path to oms-app.jar         (default oms-app/target/oms-app.jar)
  E2E_WORKDIR     scratch dir                 (default mkdtemp)
  E2E_PORT_BASE   cluster port base           (default 19000; live box uses 9000)
  E2E_OMS_HTTP / E2E_OMS_GRPC / E2E_EGRESS_PORT   (defaults 18080/19091/19093)
  E2E_PG_HOST/PORT/DB/USER/PASSWORD           (defaults localhost/5432/oms_e2e/oms/-)
  E2E_REDIS_HOST/PORT                         (default: spawn redis-server on 16379)
"""
import atexit
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, ".."))

# ---------- config ----------
MATCH_JAR = os.path.abspath(os.environ.get(
    "E2E_MATCH_JAR", os.path.join(REPO, "..", "match", "match-cluster", "target", "match-cluster.jar")))
OMS_JAR = os.path.abspath(os.environ.get(
    "E2E_OMS_JAR", os.path.join(REPO, "oms-app", "target", "oms-app.jar")))
SCHEMA_SQL = os.path.join(
    REPO, "oms-persistence", "src", "main", "resources", "db", "migration", "V001__init_schema.sql")

PORT_BASE = int(os.environ.get("E2E_PORT_BASE", "19000"))
OMS_HTTP = int(os.environ.get("E2E_OMS_HTTP", "18080"))
OMS_GRPC = int(os.environ.get("E2E_OMS_GRPC", "19091"))
EGRESS_PORT = int(os.environ.get("E2E_EGRESS_PORT", "19093"))

PG_HOST = os.environ.get("E2E_PG_HOST", "localhost")
PG_PORT = os.environ.get("E2E_PG_PORT", "5432")
PG_DB = os.environ.get("E2E_PG_DB", "oms_e2e")
PG_USER = os.environ.get("E2E_PG_USER", "oms")
PG_PASSWORD = os.environ.get("E2E_PG_PASSWORD", os.environ.get("PGPASSWORD", ""))

REDIS_HOST = os.environ.get("E2E_REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("E2E_REDIS_PORT", "0"))  # 0 = spawn private on 16379

# Pin every spawned process to these CPUs (taskset -c syntax, e.g. "20-23").
# On a dev box that also runs the live stack this is close to mandatory:
# an unpinned 3-node cluster can starve the desktop AND the live cluster's
# 1s leader heartbeat (observed 2026-07-05: box froze, live leader blipped).
CPUSET = os.environ.get("E2E_CPUSET", "")

BASE = f"http://127.0.0.1:{OMS_HTTP}"
FP = 10 ** 8  # 8-dp fixed point

# Run-scoped users so a shared Redis never bleeds balances between runs.
RUN_TAG = int(time.time()) % 100000
BUYER = 900_000_000 + RUN_TAG * 2
SELLER = BUYER + 1

PRICE = 100_000 * FP          # $100,000 — inside the BTC band, on the $1 tick
QTY = FP // 100               # 0.01 BTC per order
PAIRS_BEFORE_KILL = 40
PAIRS_AFTER_KILL = 40

procs = {}     # name -> Popen
WORKDIR = None
LOGDIR = None


def log(msg):
    print(f"[e2e {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def fail(msg):
    log(f"FAIL: {msg}")
    dump_logs()
    sys.exit(1)


def dump_logs():
    for name in sorted(os.listdir(LOGDIR)) if LOGDIR and os.path.isdir(LOGDIR) else []:
        path = os.path.join(LOGDIR, name)
        print(f"\n===== tail {name} =====", flush=True)
        with open(path, "rb") as f:
            f.seek(max(0, os.path.getsize(path) - 8000))
            sys.stdout.write(f.read().decode(errors="replace"))
    sys.stdout.flush()


# ---------- process management ----------

def spawn(name, cmd, env=None, cwd=None):
    logfile = open(os.path.join(LOGDIR, f"{name}.log"), "ab")
    full_env = dict(os.environ)
    if env:
        full_env.update(env)
    if CPUSET and shutil.which("taskset"):
        cmd = ["taskset", "-c", CPUSET] + cmd
    p = subprocess.Popen(cmd, stdout=logfile, stderr=subprocess.STDOUT,
                         env=full_env, cwd=cwd or WORKDIR)
    procs[name] = p
    log(f"spawned {name} pid={p.pid}")
    return p


def kill9(name):
    p = procs.get(name)
    if p and p.poll() is None:
        os.kill(p.pid, signal.SIGKILL)
        p.wait()
    log(f"killed -9 {name}")


def teardown():
    for name, p in procs.items():
        if p.poll() is None:
            p.terminate()
    deadline = time.time() + 5
    for p in procs.values():
        try:
            p.wait(timeout=max(0.1, deadline - time.time()))
        except subprocess.TimeoutExpired:
            p.kill()
    # remove the aeron-dir subtree so /dev/shm doesn't leak
    if WORKDIR:
        shutil.rmtree(shm_base(), ignore_errors=True)


atexit.register(teardown)


# ---------- helpers ----------

def java_cmd():
    return [os.environ.get("JAVA", "java"),
            "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens", "java.base/java.nio=ALL-UNNAMED"]


def psql(sql, db=None):
    env = dict(os.environ)
    if PG_PASSWORD:
        env["PGPASSWORD"] = PG_PASSWORD
    r = subprocess.run(
        ["psql", "-h", PG_HOST, "-p", PG_PORT, "-U", PG_USER, "-d", db or PG_DB,
         "-v", "ON_ERROR_STOP=1", "-tA", "-F", "|", "-c", sql],
        capture_output=True, text=True, env=env)
    if r.returncode != 0:
        raise RuntimeError(f"psql failed: {r.stderr.strip()}\nSQL: {sql}")
    return [line for line in r.stdout.splitlines() if line]


def psql_file(path):
    env = dict(os.environ)
    if PG_PASSWORD:
        env["PGPASSWORD"] = PG_PASSWORD
    r = subprocess.run(
        ["psql", "-h", PG_HOST, "-p", PG_PORT, "-U", PG_USER, "-d", PG_DB,
         "-v", "ON_ERROR_STOP=1", "-q", "-f", path],
        capture_output=True, text=True, env=env)
    if r.returncode != 0:
        raise RuntimeError(f"psql -f failed: {r.stderr.strip()}")


def http(method, path, body=None, user=None, timeout=5):
    req = urllib.request.Request(BASE + path, method=method)
    req.add_header("Authorization", f"Bearer dev:{user or BUYER}")
    data = None
    if body is not None:
        req.add_header("Content-Type", "application/json")
        data = json.dumps(body).encode()
    try:
        with urllib.request.urlopen(req, data=data, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode() or "{}")
        except Exception:
            return e.code, {}
    except Exception as e:
        return 0, {"error": str(e)}


def wait_for(desc, fn, timeout, interval=1.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            v = fn()
            # not-None success semantics: a leaderMemberId of 0 is a value
            if v is not None and v is not False:
                return v
        except Exception:
            pass
        time.sleep(interval)
    fail(f"timeout after {timeout}s waiting for {desc}")


def port_free(port):
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


def money(fp_value):
    sign = "-" if fp_value < 0 else ""
    v = abs(fp_value)
    return f"{sign}{v // FP}.{v % FP:08d}"


def parse_money(s):
    sign = -1 if s.startswith("-") else 1
    s = s.lstrip("-")
    whole, _, frac = s.partition(".")
    return sign * (int(whole) * FP + int((frac or "0").ljust(8, "0")[:8]))


# ---------- cluster ----------

def cluster_env(node_id):
    return {
        "CLUSTER_NODE": str(node_id),
        "CLUSTER_ADDRESSES": "127.0.0.1,127.0.0.1,127.0.0.1",
        "CLUSTER_PORT_BASE": str(PORT_BASE),
        "BASE_DIR": os.path.join(WORKDIR, f"node{node_id}"),
        # Never inherit a dev box's external-driver setup
        "TRANSPORT_DRIVER_MODE": "embedded",
        "TRANSPORT_IDLE_MODE": "backoff",
        # 16m terms × every cluster channel ≈ gigabytes of /dev/shm; that
        # froze a 31G dev box (page-reclaim storm stalled the LIVE cluster
        # too) and would blow a CI runner's ~3.5G shm. 1m is plenty at E2E rates.
        "TRANSPORT_TERM_LENGTH": os.environ.get("E2E_TERM_LENGTH", "1m"),
        # The consensus LOG channel is NOT covered by TRANSPORT_TERM_LENGTH:
        # Aeron's default is 64m, and every catch-up replay maps + zero-faults
        # a fresh non-sparse 3-term buffer (192MB observed live). Election /
        # rejoin churn on a small CI runner spends whole connect-timeouts
        # faulting buffers -> "no connection established for replayChannel"
        # -> no leader / no rejoin (oms#73, both failure modes). 4m (12MB per
        # buffer) is ample at E2E rates.
        "TRANSPORT_LOG_TERM_LENGTH": os.environ.get("E2E_LOG_TERM_LENGTH", "4m"),
        # keep clear of a live stack's 9500..9502
        "METRICS_PORT": str(PORT_BASE + 500 + node_id),
    }


def shm_base():
    # aeron dirs want page-cache-free mmap; /dev/shm when we have it. The
    # subtree is removed in teardown.
    root = "/dev/shm" if os.path.isdir("/dev/shm") else os.path.join(WORKDIR, "shmfall")
    return os.path.join(root, f"oms-e2e-{RUN_TAG}")


def start_node(node_id):
    os.makedirs(os.path.join(WORKDIR, f"node{node_id}"), exist_ok=True)
    os.makedirs(shm_base(), exist_ok=True)
    cmd = java_cmd() + [
        "-Xms256m", "-Xmx1g",
        # Isolated aeron dirs: embedded nodes derive <aeron.dir>-<id>-driver.
        # Without this they'd land on /dev/shm/aeron-<user>-N-driver — the
        # LIVE cluster's driver dirs on a dev box (with dirDeleteOnStart!).
        f"-Daeron.dir={os.path.join(shm_base(), 'aeron')}",
        # One driver thread + shared archive agent per node instead of six
        # dedicated hot threads: an E2E run must never starve the host
        # (4-vCPU CI runners; dev boxes running a live latency-sensitive stack).
        "-Daeron.threading.mode=SHARED",
        "-Daeron.archive.threading.mode=SHARED",
        "-jar", MATCH_JAR,
    ]
    return spawn(f"node{node_id}", cmd, env=cluster_env(node_id))


def cluster_tool(node_id, verb):
    cluster_dir = os.path.join(WORKDIR, f"node{node_id}", "cluster")
    r = subprocess.run(
        java_cmd() + [
            # Without this the tool waits ~forever on a mid-election cluster,
            # and one find_leader() pass can eat the whole wait budget.
            "-Daeron.cluster.tool.timeout=5000",
            "-Daeron.client.liveness.timeout=5000000000",
            "-cp", MATCH_JAR, "io.aeron.cluster.ClusterTool", cluster_dir, verb],
        capture_output=True, text=True, timeout=15)
    return r.stdout + r.stderr


def find_leader(exclude=None):
    """leaderMemberId agreed by >=2 live nodes' consensus views (a single
    node's view can be stale around elections — run 7 killed a follower
    because of one)."""
    votes = {}
    for nid in (0, 1, 2):
        if nid == exclude:
            continue
        p = procs.get(f"node{nid}")
        if not p or p.poll() is not None:
            continue
        try:
            out = cluster_tool(nid, "list-members")
        except subprocess.TimeoutExpired:
            continue
        m = re.search(r"leaderMemberId=(\d+)", out)
        if m and int(m.group(1)) >= 0:
            votes[int(m.group(1))] = votes.get(int(m.group(1)), 0) + 1
    for leader, count in votes.items():
        if count >= 2:
            return leader
    return None


# ---------- OMS ----------

def start_oms():
    env = {
        "OMS_HTTP_PORT": str(OMS_HTTP),
        "OMS_GRPC_PORT": str(OMS_GRPC),
        "OMS_AUTH_MODE": "dev",
        "OMS_POSTGRES_URL": f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}",
        "OMS_POSTGRES_USER": PG_USER,
        "OMS_POSTGRES_PASSWORD": PG_PASSWORD,
        "OMS_REDIS_HOST": REDIS_HOST,
        "OMS_REDIS_PORT": str(REDIS_PORT),
        "OMS_AUDIT_LOG": "off",
        "OMS_NODE_ID": "7",
        "CLUSTER_ADDRESSES": "127.0.0.1,127.0.0.1,127.0.0.1",
        "CLUSTER_PORT_BASE": str(PORT_BASE),
        "EGRESS_HOST": "127.0.0.1",
        "EGRESS_PORT": str(EGRESS_PORT),
    }
    cmd = java_cmd() + ["-Xms256m", "-Xmx768m", "-jar", OMS_JAR]
    return spawn("oms", cmd, env=env)


def oms_healthy():
    code, body = http("GET", "/api/v1/health")
    return code == 200 and body.get("clusterConnected") is True


# ---------- load ----------

def submit(user, side, price_fp, qty_fp):
    code, body = http("POST", "/api/v1/orders", {
        "marketId": 1, "side": side, "orderType": "LIMIT",
        "price": money(price_fp), "quantity": money(qty_fp),
    }, user=user)
    return code in (200, 201) and body.get("accepted") is True


def submit_pairs(n, label, min_accept=None, timeout=120):
    """Submit n crossing SELL/BUY pairs; returns accepted order count."""
    accepted = 0
    deadline = time.time() + timeout
    submitted = 0
    while submitted < n and time.time() < deadline:
        if submit(SELLER, "SELL", PRICE, QTY):
            accepted += 1
        if submit(BUYER, "BUY", PRICE, QTY):
            accepted += 1
        submitted += 1
        time.sleep(0.05)
    log(f"{label}: {accepted} orders accepted of {2 * submitted} submitted")
    if min_accept is not None and accepted < min_accept:
        fail(f"{label}: only {accepted} accepted (need >= {min_accept})")
    return accepted


def open_orders(user):
    code, body = http("GET", f"/api/v1/orders?userId={user}", user=user)
    return body if code == 200 and isinstance(body, list) else []


def drain_open_orders(timeout=120):
    """Cancel the resting tail until both users have zero open orders."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        remaining = []
        for user in (BUYER, SELLER):
            for o in open_orders(user):
                remaining.append((user, o["omsOrderId"], o["status"]))
        if not remaining:
            return
        for user, oid, status in remaining:
            http("DELETE", f"/api/v1/orders/{oid}", user=user)
        time.sleep(2)
    fail(f"open orders did not drain: {remaining}")


# ---------- assertions ----------

def assert_all_terminal():
    for user in (BUYER, SELLER):
        code, hist = http("GET", "/api/v1/orders/history?limit=1000", user=user)
        if code != 200:
            fail(f"orders/history returned {code} for {user}")
        non_terminal = [o for o in hist
                        if o["status"] not in ("FILLED", "CANCELLED", "REJECTED", "EXPIRED")]
        if non_terminal:
            fail(f"non-terminal orders remain for {user}: "
                 f"{[(o['omsOrderId'], o['status']) for o in non_terminal]}")
        log(f"history({user}): {len(hist)} orders, all terminal")


def assert_fill_sums():
    rows = psql(f"""
        SELECT o.oms_order_id, o.filled_qty, COALESCE(SUM(e.quantity), 0)
        FROM orders o LEFT JOIN executions e ON e.oms_order_id = o.oms_order_id
        WHERE o.user_id IN ({BUYER}, {SELLER})
        GROUP BY o.oms_order_id, o.filled_qty
        HAVING o.filled_qty <> COALESCE(SUM(e.quantity), 0)""")
    if rows:
        fail(f"filledQty != SUM(executions) for {len(rows)} orders, e.g. {rows[:5]}")
    log("fill sums: every order's filledQty == SUM(executions)")


def assert_trade_pairing():
    rows = psql(f"""
        SELECT trade_id FROM executions
        WHERE user_id IN ({BUYER}, {SELLER})
        GROUP BY trade_id
        HAVING COUNT(*) <> 2
            OR COUNT(DISTINCT side) <> 2
            OR COUNT(DISTINCT price) <> 1
            OR COUNT(DISTINCT quantity) <> 1""")
    if rows:
        fail(f"unpaired/mismatched trades: {rows[:10]}")
    n = int(psql(f"SELECT COUNT(DISTINCT trade_id) FROM executions "
                 f"WHERE user_id IN ({BUYER}, {SELLER})")[0])
    if n == 0:
        fail("no trades executed at all")
    log(f"trade pairing: {n} trades, every one exactly one BUY + one SELL, equal price/qty")


def execution_deltas(user):
    usd = btc = 0
    for row in psql(f"SELECT side, price, quantity FROM executions WHERE user_id = {user}"):
        side, price, qty = row.split("|")
        price, qty = int(price), int(qty)
        notional = (price * qty) // FP  # exact: trunc division matches FixedPoint.multiply
        if side == "BUY":
            usd -= notional
            btc += qty
        else:
            usd += notional
            btc -= qty
    return usd, btc


def assert_balances(seeds):
    for user in (BUYER, SELLER):
        usd_delta, btc_delta = execution_deltas(user)
        expected = {0: seeds[user][0] + usd_delta, 1: seeds[user][1] + btc_delta}
        code, body = http("GET", f"/api/v1/accounts/{user}", user=user)
        if code != 200:
            fail(f"accounts returned {code} for {user}")
        actual = {a["assetId"]: (parse_money(a["available"]), parse_money(a["locked"]))
                  for a in body.get("assets", [])}
        for asset_id, exp_total in expected.items():
            avail, locked = actual.get(asset_id, (0, 0))
            if locked != 0:
                fail(f"user {user} asset {asset_id}: locked={locked} after drain (hold leak)")
            if avail != exp_total:
                fail(f"user {user} asset {asset_id}: balance {money(avail)} != "
                     f"expected {money(exp_total)} (seed{'+' if (exp_total - seeds[user][asset_id]) >= 0 else ''}"
                     f"{money(exp_total - seeds[user][asset_id])} from executions)")
        log(f"balances({user}): exact to the satoshi, locked=0")


def assert_positions():
    for user in (BUYER, SELLER):
        _, btc_delta = execution_deltas(user)
        code, body = http("GET", "/api/v1/positions", user=user)
        if code != 200:
            fail(f"positions returned {code} for {user}")
        net = 0
        for p in body:
            if p["marketId"] == 1:
                net = parse_money(p["netQuantity"])
        if net != btc_delta:
            fail(f"user {user}: /positions net {money(net)} != executions net {money(btc_delta)}")
    log("positions: /api/v1/positions == net of executions for both users")


# ---------- main ----------

def main():
    global WORKDIR, LOGDIR, REDIS_PORT

    if PG_DB in ("oms", "postgres", "template0", "template1"):
        fail(f"refusing to run against database '{PG_DB}'")
    for path, what in ((MATCH_JAR, "match-cluster.jar (E2E_MATCH_JAR)"),
                       (OMS_JAR, "oms-app.jar (E2E_OMS_JAR)"),
                       (SCHEMA_SQL, "schema sql")):
        if not os.path.isfile(path):
            fail(f"missing {what}: {path}")

    WORKDIR = os.environ.get("E2E_WORKDIR") or tempfile.mkdtemp(prefix="oms-e2e-")
    os.makedirs(WORKDIR, exist_ok=True)
    LOGDIR = os.path.join(WORKDIR, "logs")
    os.makedirs(LOGDIR, exist_ok=True)
    log(f"workdir={WORKDIR} portBase={PORT_BASE} users={BUYER}/{SELLER}"
        + (f" cpuset={CPUSET}" if CPUSET else ""))

    for port in (OMS_HTTP, OMS_GRPC, EGRESS_PORT):
        if not port_free(port):
            fail(f"port {port} is in use — set E2E_* port overrides")

    # -- Postgres: create db if missing (CI), then reset schema
    try:
        psql("SELECT 1")
    except RuntimeError:
        log(f"creating database {PG_DB}")
        psql(f'CREATE DATABASE "{PG_DB}"', db="postgres")
    psql("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    psql_file(SCHEMA_SQL)
    log(f"postgres ready: {PG_DB} schema reset")

    # -- Redis: reuse if given, else spawn a private one
    if REDIS_PORT == 0:
        REDIS_PORT = 16379
        if port_free(REDIS_PORT):
            if not shutil.which("redis-server"):
                fail("redis-server not found and E2E_REDIS_PORT not set")
            spawn("redis", ["redis-server", "--port", str(REDIS_PORT),
                            "--save", "", "--appendonly", "no",
                            "--dir", WORKDIR])
            wait_for("redis", lambda: not port_free(REDIS_PORT), 15)
        log(f"redis on {REDIS_PORT} (private)")
    else:
        log(f"redis on {REDIS_PORT} (external)")

    # -- 3-node cluster
    for nid in (0, 1, 2):
        start_node(nid)
    leader = wait_for("initial leader election", find_leader, 90, interval=2)
    log(f"cluster up, leader = node{leader}")

    # -- OMS
    start_oms()
    wait_for("OMS healthy + cluster-connected", oms_healthy, 90, interval=2)
    log("OMS connected to cluster")

    # -- Seed balances (exact strings; deposits echo back)
    seeds = {BUYER: (300_000 * FP, 0), SELLER: (0, 5 * FP)}  # (USD, BTC)
    for user, (usd, btc) in seeds.items():
        if usd:
            code, body = http("POST", f"/api/v1/accounts/{user}/deposit",
                              {"assetId": 0, "amount": money(usd)}, user=user)
            assert code == 200 and body.get("success"), f"USD seed failed: {code} {body}"
        if btc:
            code, body = http("POST", f"/api/v1/accounts/{user}/deposit",
                              {"assetId": 1, "amount": money(btc)}, user=user)
            assert code == 200 and body.get("success"), f"BTC seed failed: {code} {body}"
    log("users seeded")

    # -- Phase A: crossing load on a healthy cluster
    submit_pairs(PAIRS_BEFORE_KILL, "phase A (pre-kill)", min_accept=PAIRS_BEFORE_KILL)

    # -- Kill the leader mid-flight
    old_leader = find_leader()
    if old_leader is None:
        fail("could not determine leader before kill")
    kill9(f"node{old_leader}")

    # -- Phase B: keep submitting through the election; the OMS may reject
    #    while disconnected — require a healthy volume to eventually land.
    accepted_after_kill = submit_pairs(
        PAIRS_AFTER_KILL, "phase B (through election)", min_accept=20, timeout=150)

    new_leader = wait_for("new leader after kill",
                          lambda: (lambda l: l if l is not None and l != old_leader else None)(
                              find_leader(exclude=old_leader)), 90, interval=2)
    if new_leader == old_leader:
        fail("leader did not change")
    log(f"failover: node{old_leader} -> node{new_leader}, "
        f"{accepted_after_kill} orders accepted through the election")
    wait_for("OMS reconnected", oms_healthy, 60, interval=2)

    # -- Restart the killed node, wait for rejoin. A kill -9 leaves the mark
    #    files' liveness heartbeat fresh for ~10s; restarting inside that
    #    window dies with "active mark file detected" and parks in the
    #    ShutdownSignalBarrier (the node0-limbo failure mode) — that one
    #    needs a fresh JVM. Slow log replication does NOT: a CI runner can
    #    legitimately spend 30s+ in "log replication has not progressed"
    #    retries, so only the explicit limbo marker triggers a kill+retry.
    node_log = os.path.join(LOGDIR, f"node{old_leader}.log")
    rejoined = False
    for attempt in range(3):
        time.sleep(15 if attempt == 0 else 10)
        log_offset = os.path.getsize(node_log) if os.path.exists(node_log) else 0
        start_node(old_leader)
        deadline = time.time() + 150
        wedged = False
        while time.time() < deadline and not wedged:
            try:
                if re.search(r"leaderMemberId=\d+", cluster_tool(old_leader, "list-members")):
                    rejoined = True
                    break
            except Exception:
                pass
            try:
                with open(node_log, "rb") as f:
                    f.seek(log_offset)
                    wedged = b"active mark file detected" in f.read()
            except OSError:
                pass
            time.sleep(3)
        if rejoined:
            break
        log(f"node{old_leader} restart attempt {attempt + 1} "
            + ("hit the active-mark-file window" if wedged else "timed out") + "; retrying")
        kill9(f"node{old_leader}")
    if not rejoined:
        fail(f"node{old_leader} failed to rejoin after restarts")
    log(f"node{old_leader} rejoined")

    # -- Quiesce: cancel the resting tail, then let reconcile settle
    time.sleep(5)
    drain_open_orders()
    time.sleep(3)
    log("drained to zero open orders")

    # -- Invariants
    assert_all_terminal()
    assert_fill_sums()
    assert_trade_pairing()
    assert_balances(seeds)
    assert_positions()

    log("PASS: failover E2E — convergence + ledger invariants hold")


if __name__ == "__main__":
    main()
