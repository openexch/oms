# Changelog

All notable changes to `oms` (the Open Exchange order management service)
are documented here. The stack (`match`, `oms`, `admin-gateway`,
`trading-ui`) is versioned together; one version spans all four repos.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

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
