# Order Management System

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![gRPC](https://img.shields.io/badge/gRPC-1.62.2-green.svg)](https://grpc.io/)

Order management system that sits between trading clients and the [Match Engine](https://github.com/openexch/match). Handles pre-trade risk checks, balance management, order lifecycle tracking, and synthetic order types — all before forwarding to the matching engine via Aeron Cluster.

## Key Features

- **7-check risk pipeline** — Rate limiting, circuit breakers, order size, price collar, open order limits, balance sufficiency, and position limits in a fail-fast pipeline
- **Double-entry ledger** — Atomic hold/release/settle with per-user locking and idempotent trade settlement
- **Synthetic order types** — Stop-loss, stop-limit, trailing stop, and iceberg orders with real-time market data monitoring
- **Order lifecycle state machine** — Full tracking from submission through risk, hold, cluster acknowledgment, fills, and terminal states
- **Order modify (cancel-and-replace)** — PUT endpoint for atomic price/quantity updates on active orders
- **GTD expiry** — Automatic cancellation of Good-Till-Date orders via scheduled timer (1s interval)
- **Dual API** — REST (Netty HTTP) and gRPC with server-side streaming for orders, executions, and balances
- **Hot-reloadable risk config** — Update risk parameters per market at runtime via admin API
- **PostgreSQL persistence** — Orders and executions persisted via HikariCP connection pool (graceful degradation if DB unavailable)
- **Redis balance store** — Optional Redis-backed balance store via Lettuce with Lua-scripted atomic operations (falls back to in-memory)
- **Fixed-point arithmetic** — 8-decimal precision (10^8 scaling) throughout, consistent with the match engine
- **5 market pairs** — BTC, ETH, SOL, XRP, DOGE against USD (shared via `MarketInfo` from match-common)

## Architecture

```
                    ┌──────────────────────────────────────────────┐
                    │              Trading Clients                  │
                    └──────┬──────────────────┬────────────────────┘
                           │ REST             │ gRPC
                    ┌──────▼──────┐    ┌──────▼──────┐
                    │  HTTP :8080 │    │  gRPC :9090 │
                    │  REST API   │    │  Streaming   │
                    └──────┬──────┘    └──────┬──────┘
                           │                  │
                    ┌──────▼──────────────────▼──────┐
                    │       OmsOrderServiceImpl       │
                    │                                 │
                    │  1. Validate                    │
                    │  2. Risk Engine (7 checks)      │
                    │  3. Ledger Hold                 │
                    │  4. Cluster Submit / Synthetic   │
                    └──────┬──────────────┬───────────┘
                           │              │
              ┌────────────▼───┐   ┌──────▼──────────────┐
              │  Synthetic     │   │   Aeron Cluster      │
              │  Order Engine  │   │   (Match Engine)     │
              │                │   │                      │
              │  Stop-loss     │   │   Ingress ──► Match  │
              │  Trailing stop │   │   Egress  ◄── Fills  │
              │  Iceberg       │   │                      │
              └────────────────┘   └──────────────────────┘
                                          │
                    ┌─────────────────────▼──────────────────────┐
                    │              OmsCoreEngine                  │
                    │                                             │
                    │  OrderLifecycleManager ─ state machine      │
                    │  SettlementHandler ─ ledger settle          │
                    │  PersistenceHandler ─ order/trade log       │
                    │  WebSocket push ─ real-time updates         │
                    └────────────────────────────────────────────┘
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Runtime | Java 21 |
| Cluster Communication | Aeron Cluster 1.48.1, SBE 1.33.1 |
| Event Processing | LMAX Disruptor 4.0.0 |
| HTTP Server | Netty 4.2 |
| gRPC | gRPC Java 1.82, Protobuf 4.35 |
| Data Structures | Agrona 2.4 (zero-allocation maps) |
| Persistence | PostgreSQL JDBC 42.7, HikariCP 7.1 |
| Caching | Lettuce (Redis) 7.6 |
| Serialization | Jackson 2.22 |
| Build | Maven 3+ |

## Quick Start

### Prerequisites

- Java 21+
- Maven 3+
- [Match Engine](https://github.com/openexch/match) running (for cluster connectivity)

### Build & Run

OMS depends on `com.match:match-common`, which is not published to Maven
Central; install it into your local repository from a sibling checkout of
[match](https://github.com/openexch/match) first.

```bash
# One-time: install match-common from the match repo
mvn -f ../match/pom.xml install -DskipTests -pl match-common -am

# Build all modules
mvn clean package -DskipTests

# Run tests (52 tests across 4 modules)
mvn test

# Start the OMS
java -jar oms-app/target/oms-app-1.0-SNAPSHOT.jar
```

The OMS starts with sensible defaults: HTTP on port 8080, gRPC on port 9090, in-memory balance store.

For durable persistence, create the database and apply the schema before
starting (the OMS does not auto-migrate; without a reachable PostgreSQL it
runs in-memory only):

```bash
createdb -U postgres oms
psql -U postgres -d oms -v ON_ERROR_STOP=1 \
  -f oms-persistence/src/main/resources/db/migration/V001__init_schema.sql
export OMS_POSTGRES_PASSWORD=...   # no default; required for persistence
```

## Project Structure

```
oms/
├── oms-common/           # Shared domain models, enums, utilities
├── oms-cluster-client/   # Aeron Cluster integration (ingress/egress)
├── oms-risk/             # Pre-trade risk engine (7-check pipeline)
├── oms-ledger/           # Double-entry ledger, balance store, settlement
├── oms-core/             # Order lifecycle manager, synthetic order engine
├── oms-persistence/      # PostgreSQL repositories (batch writer)
├── oms-api/              # REST, gRPC, and WebSocket API layer
├── oms-app/              # Application entry point, component wiring
└── oms-loadtest/         # HTTP load generator with metrics
```

## Order Flow

```
1. Client POST /api/v1/orders ─► OmsOrderServiceImpl
2. Validate request (userId, marketId, enums)
3. Register order ─► PENDING_RISK
4. Risk Engine: rate limit ► circuit breaker ► size ► price collar ► open orders ► balance ► position
5. Ledger hold (atomic available ─► locked) ─► PENDING_HOLD ─► PENDING_NEW
6a. Synthetic? ─► SyntheticOrderEngine (monitor market data for trigger)
6b. Native? ─► ClusterClient.submitOrder() via Aeron ingress
7. Match Engine fills ─► Egress ─► OmsCoreEngine.onTradeExecution()
8. Settle: debit locked from buyer, credit base to buyer, credit quote to seller
9. WebSocket push + gRPC stream to client
```

## API

### Create Order

Money crosses the wire as exact 8-dp decimal strings and 64-bit ids as JSON
strings — the full frozen contract lives in [docs/API.md](docs/API.md)
([OpenAPI](docs/openapi.yaml); internal SBE wire: [docs/PROTOCOLS.md](docs/PROTOCOLS.md)).

```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "marketId": 1,
    "side": "BUY",
    "orderType": "LIMIT",
    "price": "50000",
    "quantity": "1.0"
  }'
```

### Cancel Order

```bash
curl -X DELETE http://localhost:8080/api/v1/orders/123456789
```

### Account Balances

```bash
curl http://localhost:8080/api/v1/accounts/1
```

### Deposit Funds

```bash
curl -X POST http://localhost:8080/api/v1/accounts/1/deposit \
  -H "Content-Type: application/json" \
  -d '{"assetId": 0, "amount": "100000"}'
```

### gRPC Streaming (Orders)

```java
StreamRequest req = StreamRequest.newBuilder().setUserId(1).build();
stub.streamOrders(req, new StreamObserver<OrderUpdate>() {
    @Override
    public void onNext(OrderUpdate update) {
        System.out.println("Order update: " + update.getOrder().getStatus());
    }
    // ...
});
```

### REST Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/orders` | Create order |
| `PUT` | `/api/v1/orders/{id}` | Update order (cancel-and-replace with new price/quantity) |
| `DELETE` | `/api/v1/orders/{id}` | Cancel order |
| `GET` | `/api/v1/orders/{id}` | Get order |
| `GET` | `/api/v1/orders?userId=X` | Query orders |
| `GET` | `/api/v1/accounts/{userId}` | Get balances |
| `POST` | `/api/v1/accounts/{userId}/deposit` | Deposit funds |
| `POST` | `/api/v1/accounts/{userId}/withdraw` | Withdraw funds |
| `GET` | `/api/v1/markets` | List markets |
| `GET` | `/api/v1/health` | Health check |

### Admin Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/admin/risk/config` | Get all risk configs |
| `GET` | `/api/v1/admin/risk/config/{marketId}` | Get market risk config |
| `PUT` | `/api/v1/admin/risk/config/{marketId}` | Update risk config (hot-reload) |
| `POST` | `/api/v1/admin/risk/circuit-breaker/{marketId}/trip` | Trip circuit breaker |
| `POST` | `/api/v1/admin/risk/circuit-breaker/{marketId}/reset` | Reset circuit breaker |

### gRPC Services

| Service | RPC | Type |
|---------|-----|------|
| OrderService | `CreateOrder` | Unary |
| OrderService | `CancelOrder` | Unary |
| OrderService | `GetOrder` | Unary |
| OrderService | `StreamOrders` | Server streaming |
| OrderService | `StreamExecutions` | Server streaming |
| AccountService | `GetBalances` | Unary |
| AccountService | `StreamBalances` | Server streaming |

## Synthetic Order Types

| Type | Trigger Condition | Child Order |
|------|-------------------|-------------|
| Stop-Loss (sell) | Best bid <= stop price | Market sell |
| Stop-Loss (buy) | Best ask >= stop price | Market buy |
| Stop-Limit | Same as stop-loss | Limit at specified price |
| Trailing Stop (sell) | Bid drops by delta from high watermark | Market sell |
| Trailing Stop (buy) | Ask rises by delta from low watermark | Market buy |
| Iceberg | Previous slice filled | Next limit slice (displayQty) |

## Configuration

All configuration via environment variables with sensible defaults:

| Variable | Default | Description |
|----------|---------|-------------|
| `OMS_HTTP_PORT` | `8080` | REST API port |
| `OMS_GRPC_PORT` | `9090` | gRPC server port |
| `OMS_NODE_ID` | `0` | Snowflake ID generator node (0-1023) |
| `OMS_REDIS_HOST` | `localhost` | Redis host (for RedisBalanceStore) |
| `OMS_REDIS_PORT` | `6379` | Redis port |
| `OMS_POSTGRES_URL` | `jdbc:postgresql://localhost:5432/oms` | PostgreSQL URL |
| `OMS_POSTGRES_USER` | `oms` | PostgreSQL user |
| `OMS_POSTGRES_PASSWORD` | `oms` | PostgreSQL password |
| `OMS_CLUSTER_INGRESS` | `localhost:9000` | Aeron Cluster ingress endpoint |
| `OMS_AUTH_MODE` | `api-key` | Auth provider: `api-key`, `jwt`, or `dev` (see below) |
| `OMS_API_KEYS` | _(empty)_ | Inline API keys: `key:userId[:ROLE1\|ROLE2];...` |
| `OMS_API_KEYS_FILE` | _(empty)_ | API-key file, one `key:userId[:roles]` entry per line, `#` comments |
| `OMS_JWT_SECRET` | _(empty)_ | HS256 secret, required when `OMS_AUTH_MODE=jwt` (`_FILE` variant supported) |
| `OMS_CORS_ORIGINS` | _(empty)_ | CORS allowlist (comma-separated origins, or `*`); empty = no CORS headers |
| `OMS_AUDIT_LOG` | `oms-audit.log` | Append-only JSONL audit log of order/account/admin mutations; `off` disables |

### Metrics (oms#38)

`GET /metrics` serves a Prometheus scrape (auth-exempt, like health — never
proxy it publicly). Micrometer-backed: REST request latency percentiles per
route, order accept/reject counters by reason, risk-check / ledger-hold /
settlement timings, egress gap counters, reconcile repair/relink totals,
active orders, WS connections, cluster connectivity, and JVM
memory/GC/thread/CPU metrics.

Note on secrets (oms#37): `OMS_POSTGRES_PASSWORD` has **no default** (the old
`oms/oms` default is gone — unset means Postgres auth fails and the OMS runs
without persistence). `OMS_POSTGRES_PASSWORD_FILE` and `OMS_JWT_SECRET_FILE`
read the value from a file for secret-store integration. TLS terminates at a
reverse proxy: see `docs/deploy-tls.md`.

## Authentication & Authorization (oms#36)

Every external surface (REST, WebSocket upgrade, gRPC) authenticates through a
pluggable seam in `oms-api`'s `auth` package: `AuthenticationProvider`
(headers → `Principal{subject, userId, roles}`) and `Authorizer`
(principal, action, resource). The caller's identity comes from the principal,
never from the request: a body/query/path `userId` is honored only when the
principal may act as that user (its own id, or the `ANY_USER` role), order-id
routes are invisible for foreign orders (404), deposit/withdraw enforce the
same ownership, and `/api/v1/admin/*` requires the `ADMIN` role. Unauthenticated
calls get 401 (gRPC `UNAUTHENTICATED`); only `GET /api/v1/health` and CORS
preflight are exempt.

Reference providers, selected by `OMS_AUTH_MODE`:

- **`api-key`** (default): static registry from `OMS_API_KEYS` /
  `OMS_API_KEYS_FILE`, sent as `Authorization: Bearer <key>` or `X-API-Key`.
  With no keys configured every request is rejected — secure by default.
- **`jwt`**: HS256 verification with `OMS_JWT_SECRET`; claims `sub` (numeric
  userId), `roles` (array), `exp` (enforced).
- **`dev`**: accepts everything; no credentials → user 1 with
  `ADMIN|ANY_USER` (keeps the demo UI and load harness working), and a
  `Bearer dev:<userId>` token selects a user. Never use outside development.

Browsers can't set arbitrary headers on a WebSocket upgrade, so a client may
offer the subprotocol list `["bearer", "<token>"]` (`new WebSocket(url,
['bearer', token])`) — the server selects `bearer` and reads the token from
the `Sec-WebSocket-Protocol` header. Tokens are deliberately NOT accepted in
the query string (URLs leak into access logs and browser history). Integrators plug real IAM
by implementing `AuthenticationProvider`/`Authorizer` and wiring them in
`OmsApplication` — KYC/session management stays the integrator's concern.

## Durability & Recovery (oms#35)

In-memory state (open orders, risk positions, open-order slots, synthetic
trigger monitoring) is rebuilt from PostgreSQL on every startup:

- **Open orders**: all non-terminal rows in `orders` are restored as-is into
  the lifecycle manager (orders interrupted before reaching the cluster
  pipeline — `PENDING_RISK`/`PENDING_HOLD` — are skipped; there is nothing to
  repair against). Stop/trailing/iceberg orders re-arm their trigger
  monitoring.
- **Positions**: replayed as a SQL aggregate over the `executions` ledger
  (BUY minus SELL per user/market).
- **Balances**: live in Redis and survive restarts on their own.

After the rebuild, the cluster open-orders snapshot reconciliation (P1.2)
trues the restored set up against cluster reality — orders the cluster no
longer knows are terminalized through the normal path, releasing holds and
slots.

**RPO**: the Postgres ledger is written synchronously on every order lifecycle
change and every execution, so OMS-side RPO is effectively zero for anything
the OMS processed. **Back up PostgreSQL** (e.g. nightly `pg_dump` + WAL
archiving in production) — it is the source of truth for money movements and
the rebuild source; losing it means positions/history are only recoverable
from the cluster's own archive (see match `docs/backup-restore.md`).

## Load Testing

```bash
# Build the load test module
mvn package -pl oms-loadtest -am -DskipTests

# Run with default settings (100 orders/sec, 30s, 4 threads)
java -jar oms-loadtest/target/oms-loadtest-1.0-SNAPSHOT.jar

# Custom configuration
java -jar oms-loadtest/target/oms-loadtest-1.0-SNAPSHOT.jar \
  --rate 500 --duration 60 --threads 8 --users 100
```

Output includes throughput, latency percentiles (p50/p95/p99/max), and error rates.

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes
4. Push to the branch and open a Pull Request

For bug reports and feature requests, please use [GitHub Issues](https://github.com/openexch/oms/issues).

## Support

This project is **free to use** under the Apache 2.0 license.

For **advanced help**, custom deployments, performance tuning, or consulting — reach out:

- **Email**: emrebulutlar@gmail.com
- **GitHub Issues**: [openexch/oms/issues](https://github.com/openexch/oms/issues)

## License

This project is licensed under the [Apache License 2.0](LICENSE).

```
Copyright 2026 Ziya Emre Bulutlar
```
