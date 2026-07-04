# Internal cluster wire protocol (SBE) — reference

The OMS and the market gateway talk to the match cluster over Aeron Cluster
ingress/egress using SBE messages defined in
`match/match-common/src/main/resources/sbe/order-schema.xml`
(**schema id 1, version 3**). This documents the semantics an integrator
embedding the OMS needs; the external API contract is [API.md](API.md).

All prices/quantities on this wire are 8-dp fixed-point `int64`
(`FixedPoint`, scale 10^8). All timestamps are epoch milliseconds.

## Ingress (client → cluster)

| Message | id | Sender | Notes |
|---------|----|--------|-------|
| `CreateOrder` | 1 | OMS | Carries `clusterOrderId` (OMS-minted correlation id) + `omsOrderId` |
| `CancelOrder` | 2 | OMS | |
| `UpdateOrder` | 3 | OMS | Cancel-and-replace semantics in the book |
| `RequestOpenOrdersSnapshot` | 7 | OMS | Ask the leader for an `OpenOrdersSnapshot` (reconciliation) |

Ingress is FIFO per session; the engine is deterministic, so identical
ingress order yields identical state on all nodes.

## Egress (cluster → clients)

Two delivery classes, chosen per stream because a slow consumer must never
stall consensus:

- **`broadcastReliable`** (OMS settlement plane): buffered per session,
  byte-bounded (~128 MB OMS + 32 MB market data end-to-end); overflow sheds
  loudly (CRITICAL log + OMS reconciliation repair) instead of OOM-ing the
  match thread. `TradeExecutionBatch` and `OrderStatusBatch` use this.
- **`broadcast`** (market-data plane): lossy by design; the gateway
  reconstructs state from snapshots. Book/trade/ticker streams use this.

| Message | id | Class | Notes |
|---------|----|-------|-------|
| `TradeExecution` | 4 | legacy single | superseded by batch 26 |
| `OrderStatusUpdate` | 5 | legacy single | superseded by batch 24 |
| `OrderBookUpdate` | 6 | broadcast | legacy level update |
| `ClusterHeartbeat` | 21 | broadcast | 1 s keep-warm, leader only |
| `BookSnapshot` | 22 | broadcast | full book (top levels) per market |
| `TradesBatch` | 23 | broadcast | aggregated trades for display |
| `OrderStatusBatch` | 24 | broadcastReliable | see **statusSeq** below |
| `BookDelta` | 25 | broadcast | incremental levels with bid/ask versions |
| `TradeExecutionBatch` | 26 | broadcastReliable | authoritative fills; `tradeId` strictly monotonic |
| `OpenOrdersSnapshot` | 27 | reliable, leader-only | chunked; see below |

### Loss detection: statusSeq and tradeId (schema v3, P1.2)

- Every `OrderStatusBatch` entry carries a per-market, monotonically
  contiguous **`statusSeq`**. The publisher consumes a seq for every status
  even when a message is dropped, so ANY wire gap is detectable by the
  consumer. Counters reset on leader change; consumers rebaseline at seams.
- `tradeId` is globally monotonic and contiguous on the reliable stream.
- On a detected gap the OMS requests an `OpenOrdersSnapshot` (rate-limited
  to one per 5 s) and reconciles.

### OpenOrdersSnapshot reconciliation

`RequestOpenOrdersSnapshot` → the leader enumerates the live books and
streams chunked `OpenOrdersSnapshot` messages (`requestId`, repeating group
of `orderId`/`omsOrderId`, `snapshotMaxOrderId`, `isLast`). The OMS:

- terminalizes local orders the cluster no longer has (releasing holds and
  risk slots through the normal status path);
- re-links orphans it can match by `omsOrderId`;
- never touches orders newer than `snapshotMaxOrderId` (in-flight cutoff).

This is the self-healing path for anything lost on the (bounded) egress.

### Fill accounting invariant

`filledQty` is derived exclusively from `TradeExecutionBatch` (the reliable,
gap-checked stream). `OrderStatusBatch` is coalesced and only acts as a
monotonic backstop. Duplicated trade delivery across leader switchovers is
idempotent on `tradeId`.

## Schema evolution rules

- SBE fields are append-only within a message; new fields declare
  `sinceVersion` (v3 added `statusSeq`, `omsOrderId` plumbing, messages 7/27).
- Message ids are never reused.
- Snapshot codecs (cluster state) are versioned separately from this wire
  schema; changing them requires a coordinated cluster upgrade (see match
  repo docs).
- Consumers must tolerate unknown messages (skip by id) so the gateway and
  OMS can upgrade independently within a schema version.
