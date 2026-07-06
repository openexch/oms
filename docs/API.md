# OMS API contract (v1) — FROZEN

Published and frozen for v0.3.0-beta (oms#39 / P4.1). Changes to anything in
this document follow the [stability policy](#api-stability-policy) below.
The machine-readable REST spec is [openapi.yaml](openapi.yaml); the gRPC
contract is `oms-api/src/main/proto/oms_services.proto`; the internal
cluster wire is documented in [PROTOCOLS.md](PROTOCOLS.md).

## Conventions

### Money: exact decimal strings

Every money value (prices, quantities, amounts, balances) crosses the wire
as a **decimal string with at most 8 fractional digits**:

```json
{ "price": "110224.00000000", "quantity": "0.10000001" }
```

- Internally money is 8-dp fixed-point (scaled `long`); the string form
  round-trips it bit-exactly. JSON numbers cannot (IEEE doubles).
- Responses always carry exactly 8 fractional digits.
- Requests may use fewer digits (`"0.1"`, `"101997"`); more than 8, exponent
  notation, signs on unsigned fields, or non-decimal input is rejected with
  `400 VALIDATION`.
- The representable range is `0` .. `92233720368.54775807`.
- **Deprecated**: JSON numbers are still accepted on input for one release
  (rounded through a double); do not rely on it.

### Ids

- `omsOrderId` and `clusterOrderId` are 64-bit Snowflake-style ids and are
  serialized as **JSON strings** (`"331884039403208704"`): they exceed
  JavaScript's 2^53-1 exact-integer range and would be silently rounded as
  numbers. Treat them as opaque strings.
- `userId`, `marketId`, `assetId` are small integers and stay JSON numbers.
- In gRPC, ids are `int64` (protobuf is 64-bit native); money is `string`.

### Errors

Every non-2xx response has the shape:

```json
{ "error": "<human-readable message>", "code": "<CODE>" }
```

| HTTP | code | Meaning |
|------|------|---------|
| 400 | `VALIDATION` | Malformed input (bad money string, missing field, out-of-range value) |
| 400 | `REJECTED` | Valid input refused by business rules (e.g. withdraw exceeding balance) |
| 401 | `UNAUTHORIZED` | Missing/invalid credentials |
| 403 | `FORBIDDEN` | Authenticated but not allowed (foreign user, missing ADMIN role) |
| 404 | `NOT_FOUND` | Unknown route, order, or market. Foreign order ids also return 404 (not probeable) |
| 503 | `ADMIN_UNAVAILABLE` | Admin subsystem not wired in this deployment |
| 503 | `UNAVAILABLE` | Required subsystem down (e.g. history reads while running without persistence) |
| 500 | `INTERNAL` | Unhandled server error |

Order **rejections** are not HTTP errors: `POST /orders` returns
`400` with a normal `CreateOrderResponse` body (`accepted: false`) whose
`rejectReason` is one of the fixed vocabulary:

`RATE_LIMIT_EXCEEDED`, `CIRCUIT_BREAKER_OPEN`, `ORDER_SIZE_TOO_SMALL`,
`ORDER_SIZE_TOO_LARGE`, `NOTIONAL_TOO_SMALL`, `NOTIONAL_TOO_LARGE`,
`PRICE_COLLAR_BREACH`, `OPEN_ORDER_LIMIT`, `INSUFFICIENT_BALANCE`,
`POSITION_LIMIT_EXCEEDED`, `HOLD_FAILED`, `CLUSTER_REJECT`, `INVALID_ORDER`,
`MARKET_HALTED`.

Known gap: orders rejected by the matching engine itself (off-tick price,
out-of-range price, book full) currently surface as a terminal `REJECTED`
status with an **empty** rejectReason — the cluster egress does not carry
the reason yet (openexch/match#64).

### clientOrderId idempotency

`clientOrderId` (optional, ≤64 chars) is a caller-chosen idempotency key,
scoped **per user across their ACTIVE orders**:

- A `POST /orders` whose `clientOrderId` matches one of the caller's active
  orders creates nothing and returns `200` (not `201`) with
  `duplicate: true` and the existing order's `omsOrderId`/`status`, so a
  retried request after a lost response is safe.
- Once that order is terminal (`FILLED`/`CANCELLED`/`REJECTED`/`EXPIRED`),
  the id may be reused; rejected submissions release it immediately.
- The dedupe window survives OMS restarts (the index is rebuilt from
  persisted open orders), but uniqueness is not enforced against terminal
  history.

### Market price rules

The engine enforces a hard price band and tick size per market; off-tick or
out-of-band prices are rejected engine-side (see the gap above). Current
values (`match-cluster` `MarketConfig`, discoverability tracked in match#64):

| Market | id | Range (USD) | Tick |
|--------|----|-------------|------|
| BTC-USD | 1 | 50,000 – 150,000 | 1.00 |
| ETH-USD | 2 | 1,000 – 10,000 | 0.50 |
| SOL-USD | 3 | 50 – 500 | 0.05 |
| XRP-USD | 4 | 0.50 – 10 | 0.001 |
| DOGE-USD | 5 | 0.05 – 1.00 | 0.0001 |

### Authentication

`Authorization: Bearer <token>` on every call. Providers: `api-key`
(`key:userId[:ROLES]`, the secure default), `jwt` (HS256), `demo`
(public demo: self-registered users, see below), `dev` (development only).
Identity is always derived from the token; caller-supplied `userId` fields
are honored only when the principal may act as that user. WebSocket browser
clients pass the token as the second `Sec-WebSocket-Protocol` offer:
`['bearer', <token>]`. Query-string tokens are rejected.
`GET /api/v1/health`, `GET /metrics`, and the demo register/login endpoints
are exempt.

**Demo mode** (`OMS_AUTH_MODE=demo`; requires Postgres with
`V002__users.sql` applied): `POST /api/v1/auth/register` and
`POST /api/v1/auth/login` take `{username, password}` and return
`{userId, username, token}` — one active opaque token per user, rotated on
login. `GET /api/v1/auth/me` echoes the authenticated identity. Registered
principals are self-scoped (no `ANY_USER`), so every endpoint below serves
only their own data; registration funds demo balances on all assets. A
`dev:<id>` backdoor remains for local infrastructure only: userId 1 (ADMIN,
self-scoped) and the simulator range 900000-900999 (self-scoped);
registered-range ids are rejected.

## REST endpoints

Base: `http://<oms>:8080`. See [openapi.yaml](openapi.yaml) for full
request/response schemas.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/orders` | Create order → `201` CreateOrderResponse / `200` `duplicate:true` / `400` with rejectReason |
| `PUT` | `/api/v1/orders/{omsOrderId}` | Cancel-and-replace price/quantity |
| `DELETE` | `/api/v1/orders/{omsOrderId}` | Cancel order |
| `GET` | `/api/v1/orders/{omsOrderId}` | Get order (terminal orders served from persistence) |
| `GET` | `/api/v1/orders?status=` | Query the principal's ACTIVE orders (a terminal `status=` is served from persistence) |
| `GET` | `/api/v1/orders/history?status=&limit=&offset=` | Order history, newest first (Postgres; `limit` ≤1000, default 100) |
| `GET` | `/api/v1/executions?limit=&offset=` | Fill history, newest first (`tradeId`/`omsOrderId` as strings) |
| `GET` | `/api/v1/positions` | Net base quantity per market, aggregated from executions (signed decimal string) |
| `GET` | `/api/v1/accounts/{userId}` | Balances (decimal strings) |
| `POST` | `/api/v1/accounts/{userId}/deposit` | Credit balance (`{assetId, amount}`) |
| `POST` | `/api/v1/accounts/{userId}/withdraw` | Debit balance; `400 REJECTED` when exceeding available |
| `GET` | `/api/v1/markets` | Market list |
| `GET` | `/api/v1/health` | Liveness (no auth) |
| `GET` | `/metrics` | Prometheus (no auth) |
| `GET/PUT` | `/api/v1/admin/risk/config[/{marketId}]` | Risk config (ADMIN role) |
| `POST` | `/api/v1/admin/risk/circuit-breaker/{marketId}/{trip\|reset}` | Circuit breaker (ADMIN role) |

Order statuses: `PENDING_RISK → PENDING_HOLD → PENDING_NEW → NEW →
PARTIALLY_FILLED → FILLED | CANCELLED | REJECTED` (+ `PENDING_TRIGGER` for
stop/trailing orders). `filledQty` derives from the authoritative trade
stream, never from coalesced status updates.

## OMS WebSocket (`/ws/v1` on :8080)

Subscribe protocol: `{"op":"subscribe","channels":["orders"],"userId":N}` —
`userId` optional (defaults to the principal's own; the principal must be
allowed to act as it). Ack: `{"type":"SUBSCRIBED","userId":N}`. Push events
are the bare `OrderResponse` JSON (same shape as REST, no type wrapper —
distinguish from acks by the presence of `omsOrderId`), delivered only to
the order owner's connections. `{"op":"ping"}` → `{"type":"PONG"}` for
client-side liveness (the server sends no heartbeats). Channels
`executions`/`balances` are declared but not yet pushed. Browser auth: pass
the token via the `Sec-WebSocket-Protocol` offer `['bearer', <token>]`.

## gRPC (`:9090`)

`OrderService`: CreateOrder, CancelOrder, GetOrder, GetOrderHistory,
GetTrades, StreamOrders, StreamExecutions. `AccountService`: GetBalances,
StreamBalances. Money = decimal strings, ids = int64, auth via metadata
`authorization: Bearer ...`. History RPCs return `UNAVAILABLE` when the OMS
runs without persistence. See `oms_services.proto`.

## Market-data plane (match-gateway, `ws://:8081/ws`)

A separate, documented-elsewhere surface (book snapshots/deltas, trades,
ticker, candles, cluster status). Note its money values are JSON numbers
(display-grade), while `omsOrderId` on `ORDER_STATUS` messages is a string.
Authoritative money lives on the OMS plane only.

## API stability policy

- The REST/gRPC/WS surfaces above are **frozen for v0.3.x**: breaking
  changes (removing/renaming fields, changing types or semantics, removing
  the deprecated number-input tolerance) require a **major** version bump of
  the API (`/api/v2`, proto package bump) and a deprecation window of one
  minor release.
- **Additive** changes (new fields, new endpoints, new enum values in
  `rejectReason`/`code`) may ship in any minor release; clients must ignore
  unknown JSON fields and tolerate unknown enum values.
- The error `code` set and reject-reason vocabulary only grow, never shrink
  or change meaning within a major version.
- Release versioning is coordinated across all four repos (single
  `vX.Y.Z-*` tag set; see match `docs/RELEASING.md`).
