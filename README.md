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
- **Dual API** — REST (Netty HTTP) and gRPC with server-side streaming for orders, executions, and balances
- **Hot-reloadable risk config** — Update risk parameters per market at runtime via admin API
- **Fixed-point arithmetic** — 8-decimal precision (10^8 scaling) throughout, consistent with the match engine
- **5 market pairs** — BTC, ETH, SOL, XRP, DOGE against USD

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
| HTTP Server | Netty 4.1.100 |
| gRPC | gRPC Java 1.62.2, Protobuf 3.25.3 |
| Data Structures | Agrona 2.2.2 (zero-allocation maps) |
| Persistence | PostgreSQL 42.7.3, HikariCP 5.1.0 |
| Caching | Lettuce (Redis) 6.3.2 |
| Serialization | Jackson 2.17.0 |
| Build | Maven 3+ |

## Quick Start

### Prerequisites

- Java 21+
- Maven 3+
- [Match Engine](https://github.com/openexch/match) running (for cluster connectivity)

### Build & Run

```bash
# Build all modules
mvn clean package -DskipTests

# Run tests (52 tests across 4 modules)
mvn test

# Start the OMS
java -jar oms-app/target/oms-app-1.0-SNAPSHOT.jar
```

The OMS starts with sensible defaults — HTTP on port 8080, gRPC on port 9090, in-memory balance store.

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

```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "userId": 1,
    "marketId": 1,
    "side": "BUY",
    "orderType": "LIMIT",
    "price": 50000.0,
    "quantity": 1.0
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
  -d '{"assetId": 0, "amount": 100000.0}'
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
