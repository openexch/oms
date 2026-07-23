# Full-stack failover E2E (oms#41 / P5.1)

`failover_e2e.py` is P1's acceptance test in miniature, runnable on a clean
clone: it launches a REAL 3-node Aeron cluster (embedded media drivers), a
REAL single-node Assets Engine (the sole balance store — the OMS refuses to
boot without it) and a REAL OMS against Postgres, streams crossing limit
orders, `kill -9`s the cluster LEADER mid-load, keeps submitting through the
election, restarts the killed node, drains to quiescence, and asserts:

- every order in history is terminal (nothing stuck open),
- per-order `filledQty == SUM(executions.quantity)` in Postgres,
- every tradeId has exactly one BUY + one SELL row with equal price/qty,
- balances equal seed ± exact execution deltas with `locked == 0`
  (integer fixed-point math vs the REST decimal strings),
- `/api/v1/positions` equals the net of executions,
- the failover was real: the leader changed and orders were accepted
  after the kill.

CI runs it in the `e2e` job (Postgres as a service). A green run takes
about 2 minutes after the builds.

## Running on a dev box

Safe next to a live stack: isolated port bases (match 19000, AE 19300),
isolated aeron dirs under `/dev/shm/oms-e2e-*`, and its own database.

```bash
# one-time: create the database (any superuser)
psql -U postgres -c 'CREATE DATABASE oms_e2e OWNER oms'

mvn -f ../match/pom.xml package -DskipTests -pl match-cluster -am
mvn -f ../assets/pom.xml package -DskipTests -pl assets-cluster -am
mvn package -DskipTests && cp oms-app/target/oms-app-1.0-SNAPSHOT.jar oms-app/target/oms-app.jar

E2E_CPUSET=20-23 E2E_PG_PASSWORD=... python3 e2e/failover_e2e.py
```

**Set `E2E_CPUSET` on a shared box.** An unpinned run can starve the desktop
and a live cluster's 1s leader heartbeat; term buffers are already shrunk to
1m (`E2E_TERM_LENGTH`) because 16m terms across a second full stack froze a
31G box via page-reclaim storms.

All knobs (`E2E_MATCH_JAR`, `E2E_ASSETS_JAR`, `E2E_OMS_JAR`,
`E2E_PORT_BASE`, `E2E_AE_PORT_BASE`, `E2E_PG_*`, `E2E_WORKDIR`) are
documented at the top of the script. The harness refuses to run against a
database named `oms`.

## Found by this harness (fixed in the same PR)

- **Stuck PENDING_NEW zombies after a quiet failover**: orders whose
  create/ack was lost at the seam stayed inside the orphan age gate during
  the post-reconnect reconcile, and nothing later triggered another one.
  Fixed with the 1s stale-orphan sweep (`OmsEgressAdapter.sweepStaleOrphans`).
- **Silently lost execution rows (oms#23)**: an instant crossing fill hit the
  `executions` FK before the order row's first upsert — 232 ground-truth
  ledger rows lost in one 160-order run. Fixed with an order-row upsert +
  one retry in the persistence handler.
- **`V001__init_schema.sql` failed on PostgreSQL 16** (partitioned
  `ledger_entries` PK lacked the partition column) — the table silently never
  existed on databases initialized without `ON_ERROR_STOP`.
