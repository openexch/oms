# Changelog

All notable changes to `oms` (the Open Exchange order management service)
are documented here. The stack (`match`, `oms`, `admin-gateway`,
`trading-ui`, `assets`) is versioned together; one version spans all five repos.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.4.0-beta] - 2026-07-22

The money release: order money moves to the Assets Engine, and the fill path
is hardened end to end.

### Added
- Money on the Assets Engine: `oms-assets-client` AE cluster transport
  (money-schema v2) (#99); `AeronAssetsBalanceStore` behind the BalanceStore
  seam (#100); explicit balance-store selection + AE store wiring/metrics
  (#101); PG balance read model — CQRS mirror off the AE projection (#104).
- `AssetsHoldReconciler`: provably-never-submitted orphan sweep (#103).
- OMS↔AE integration tests — hold/amend/settle/residual/synthetic-flag end to
  end (#102).
- Ordered egress, client side: egressSeq threading + reorder metric (#98);
  engine reject reasons surfaced from SBE v6 egress (#95).
- Market metadata (tickSize/minPrice/maxPrice) served in
  `GET /api/v1/markets` (#105).
- Demo auth mode — self-registered users with scoped principals
  (`OMS_AUTH_MODE=demo`) (#72).

### Fixed
- The gap storm: out-of-order trade arrival tolerated — no longer swallowing
  ~16% of fills (#107); reconciler ignores AE tombstones, handled FK-race log
  deduped (#108).
- clusterOrderId recorded on trade-driven fills so instant-cross orders don't
  persist as 0 (#122).
- Hold/settle integrity: floor-guarded settle — an under-held order can't
  drive locked negative (#88); open-order slot released on FILLED, not just
  cancels (#112); amend/iceberg hold integrity + synthetic thread-safety
  (#96); age-gated release-bearing membership-repair terminalization (#97).
- Order lifecycle: PUT amend spans the cancel-and-replace legs — no lost
  orders (#90); queue-full rejections terminalize immediately — no
  PENDING_NEW zombies (#92); ICEBERG submits its first display slice at
  creation (#87) and multi-slice refill works on both sides of a fill; slice
  FILLED no longer terminalizes the parent (#94); market/stop BUY no longer
  misrejected on balance — hold ran before the price estimate (#83).
- Per-market risk config + manual circuit-breaker trips persist across
  restarts (#113).
- Cluster client: egress polling interleaved with order-submit back-pressure
  (#123); old AeronCluster teardown guaranteed on every reconnect path (#115).
- REST/WS plane: real HTTP/1.1 keep-alive (#93); user WS accepts more than
  one connection per OMS lifetime (#69); shared order/risk state made
  thread-safe (#71).

### Changed
- The dead in-process double-entry ledger path is deleted — the Assets Engine
  is the money authority; audit-log default path + rotation (#114).
- Aeron 1.52.2, Agrona 2.5.0, SBE 1.39.0, PostgreSQL 42.7.13, Jackson bom
  2.22.1; netty 4.2.16.Final (CVE-2026-44891), earlier pinned via bom import
  (CVE-2026-44249); jackson-core GHSA-r7wm-3cxj-wff9 fixed by the bom bump (#75-#79, #91, #117, #118, #121).
- Contact email is info@openexch.io (#80).

## [0.3.0-beta] - 2026-07-05

The beta hardening release: correctness and durability first, then security,
a frozen API contract, and a full-stack failover E2E gate in CI.

### Fixed
- FixedPoint overflow handled at every OMS money-math admission site;
  overflowing requests are rejected, never truncated (#46).
- Slot and hold leaks under election churn: broadened terminal release plus
  post-reconcile slot rebaseline, and removal of the `PENDING_TRIGGER`
  double-release (#50, #52).
- Orders left without a `clusterOrderId` across all submitted states are now
  reconciled: relinked when the cluster knows them, terminalized when it does
  not (#56).
- Three bugs found by the failover E2E (#62): stale submitted orphans are now
  swept via a rate-limited resnapshot instead of sitting `PENDING_NEW`
  forever; the executions-insert FK race with instant fills no longer loses
  ledger rows; the `V001` schema's partitioned `ledger_entries` primary key
  now includes `entry_time` so it applies cleanly on PostgreSQL 16.

### Added
- Egress gap detection and open-order membership repair against
  authoritative cluster snapshots (#47).
- Startup rebuild of positions and open orders from Postgres, with
  order-persist upsert (#48).
- Pluggable AuthN/AuthZ SPI: `userId` derived from the authenticated
  principal, secure by default (`api-key`, `jwt`, and explicit `dev` modes)
  (#54).
- Edge hardening: CORS allowlist, secrets-from-file, input validation, and
  an audit log (#55).
- Micrometer plus Prometheus `/metrics` (#57).
- Frozen public API contract: money as exact decimal strings, ids as JSON
  strings, documented error taxonomy (`docs/API.md`, `docs/openapi.yaml`)
  (#58, #59).
- History read APIs (REST and gRPC) and restart-surviving `clientOrderId`
  idempotency (#60).
- Full-stack failover E2E in CI: real 3-node cluster plus OMS, Postgres, and
  Redis; kills the leader mid-load and asserts exact fills, balances, and
  ledger conservation (#62).

### Security
- Trivy dependency and secret scanning in CI (#61).
- Dependency updates including PostgreSQL JDBC 42.7.11 (#42-#45).

## [0.2.0-alpha] - 2026-06-28

- Pending-cancel reconciliation after leader switchover (oms#21).
- Exact `filledQty` derived from the authoritative `TradeExecution` stream
  (bug9).
- Protocol-level cluster keep-alive replacing per-second log noise.

## [0.1.0-alpha] - 2026-05-08

- First tagged release: REST/gRPC order edge, risk checks, balances and
  holds, Postgres ledger, Aeron cluster client to the matching engine.

[0.3.0-beta]: https://github.com/openexch/oms/compare/v0.2.0-alpha...v0.3.0-beta
[0.2.0-alpha]: https://github.com/openexch/oms/compare/v0.1.0-alpha...v0.2.0-alpha
[0.1.0-alpha]: https://github.com/openexch/oms/releases/tag/v0.1.0-alpha
