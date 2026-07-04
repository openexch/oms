# Contributing to oms

Thanks for your interest in Open Exchange. `oms` is the order management
service: REST/gRPC edge, risk checks, balances and holds, Postgres ledger,
and the Aeron cluster client that talks to the `match` matching engine.

## Before you start

- For anything larger than a small fix, open an issue first and outline the
  approach.
- Bug reports: include the auth mode, whether persistence was enabled, and
  reproduction steps or a failing test.

## Development setup

- Java 21 (built and tested on Temurin/Zulu 21).
- Maven 3.9+. Multi-module build (`oms-app`, `oms-core`, `oms-api`,
  `oms-cluster-client`, `oms-persistence`, `oms-ledger`, `oms-risk`,
  `oms-common`, `oms-loadtest`).
- PostgreSQL 14+ and Redis for running the full service locally
  (unit tests do not need them).

```bash
mvn -B clean verify
```

CI runs the same command, Trivy dependency and secret scanning, and the
full-stack failover E2E (`e2e/failover_e2e.py`): a real 3-node cluster plus
OMS, Postgres, and Redis; it kills the leader mid-load and asserts exact
fills, balances, and ledger conservation. See `e2e/README.md` for running it
locally. If your change touches order state, reconciliation, or persistence,
run the E2E before opening the PR.

## Design constraints

- **The API contract is frozen.** `docs/API.md` and `docs/openapi.yaml` are
  the contract: money as exact decimal strings, ids as JSON strings, the
  documented error taxonomy. Wire-visible changes must update the contract
  docs in the same PR and stay backward compatible.
- **Money math is exact.** All quantities and prices go through `FixedPoint`;
  overflow must reject at admission, never truncate.
- **The Postgres ledger is ground truth.** Changes to persistence or
  reconciliation must keep `filledQty == SUM(executions)` and balance
  conservation intact; the E2E asserts both.

## Pull requests

- **One logical change per PR.** Each PR is squash-merged into exactly one
  commit on `main`.
- **Sign your commits.** `main` requires signed commits; unsigned PR heads
  cannot be merged.
- Commit/PR title style: `type: imperative summary` with types
  `feat|fix|docs|test|ci|chore|perf`.
- Reference issues in the body (`Closes #NN`).

## License

Apache-2.0. By contributing, you agree that your contributions are licensed
under the same terms as the project (inbound = outbound). No CLA.
