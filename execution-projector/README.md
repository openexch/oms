# Execution projector

C++20 worker that replays a pinned ME settlement-journal recording through
Aeron Archive and projects execution history into PostgreSQL. The worker
performs no settlement, hold release, synthetic order submission, or parent
order terminalization.

Each database transaction includes both execution legs, raw journal events,
ME-leg terminal records, and the physical replay checkpoint. Duplicate replay
must match its original payload. Existing legacy execution rows must match
exact economic fields; their original arrival timestamps are preserved.
Missing order intents, gaps, conflicting payloads, and unknown wire versions
halt consumption without advancing the transaction's checkpoint.

The source identity and recording descriptor are pinned. A different node's
position is never substituted for the current checkpoint. Recording rotation,
source failover, and retention registration require a separate verified
handoff; this executable stops if that recovery is needed.

## Build

Use CMake >= 3.30, a C++20 compiler, Java 21 for build-time SBE generation,
PostgreSQL client headers/library, and the verified dependencies in
`dependencies.lock`. Verify downloaded package hashes before extraction.
Runtime does not require Java. The Java class under `tests/` is an Archive
fixture for interoperability tests with the existing ME producer.

```sh
cmake -S execution-projector -B build/projector \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DOE_AERON_SOURCE_DIR="$AERON_SOURCE" \
  -DOE_SBE_CLASSPATH="$SBE_JAR:$AGRONA_JAR"
cmake --build build/projector --target execution-projector projector-store-test projector-monitor-test -j 2
OE_PROJECTOR_TEST_PG='service=projector_test' ctest --test-dir build/projector -V
```

The database test creates and drops only a process-specific test schema. It
injects a failure on the maker write, tests process exit after COMMIT, replays
duplicates, and checks gap, payload, source, and concurrent-writer fences.
Missing test database configuration produces a visible skipped test, not an
integration PASS. HTTP tests exercise actual loopback probes.

## Run

Stop OMS admission and the legacy execution writer. Apply
`../oms-persistence/src/main/resources/db/migration/V007__execution_writer_ownership.sql`
and `schema/projector.sql` explicitly with `psql -v ON_ERROR_STOP=1`. The migration rejects conflicting
legacy duplicate legs instead of deleting history. The existing `orders`
table and its durable intents are required; the worker does not invent them.

```sh
export OE_PROJECTOR_PG='service=execution_projector'
build/projector/execution-projector \
  --aeron-dir "$AERON_DIR" \
  --control-channel "$ARCHIVE_CONTROL_CHANNEL" --control-stream "$ARCHIVE_CONTROL_STREAM" \
  --response-channel "$ARCHIVE_RESPONSE_CHANNEL" \
  --replay-channel "$ARCHIVE_REPLAY_CHANNEL" --replay-stream "$ARCHIVE_REPLAY_STREAM" \
  --recording "$JOURNAL_RECORDING_ID" --source "$VERIFIED_SOURCE_GENERATION" \
  --mode once --monitor-port 8097
```

`once` replays to the observed recorded position and exits. `follow` keeps the
replay open on that same recording. Both start at PostgreSQL's committed
cursor, never `MAX(trade_id)`. Failed or uncertain database commits terminate
the worker; a supervisor may restart it from the durable cursor. Connections
and SQL use bounded timeouts. There is no unbounded reconnect loop.

Archive polling and database I/O run on separate threads. The queue holds at
most 1,024 events, with at most another 256 in a database batch. Queue pressure
returns Aeron controlled-poll ABORT rather than dropping events.

The monitor binds only to loopback. `/health` reports poll-loop liveness;
`/ready` also requires an active replay connection, a fresh database probe,
an empty outstanding queue, and a committed cursor at the latest observed
recorded position. `/metrics` exposes checkpoint, target, lag, outstanding
events, readiness, and failure. Database probes continue on idle recordings.
The high-water mark is refreshed every second; stale observations fail ready
after three seconds.

## Integration boundary

The database now fences legacy writers, including binaries unaware of this
protocol. `execution_writer_ownership` starts as `legacy` at epoch 1. Handoffs
must pass through `paused`; that state rejects every execution mutation. The
transition locks the ownership row until commit, so it cannot overtake an
in-flight execution write. Use the expected owner/epoch and a concrete audit
reason with `transition_execution_writer`; stale operator commands fail.

The Archive worker starts only with owner `archive`. It pins the epoch in its
connection, validates it on every batch and idle probe, and retains the separate
advisory lock excluding a second C++ worker. Pausing or changing epoch fences
both execution writes and checkpoint-only/terminal batches. Re-enabling archive
ownership never revives an old worker epoch.

For an isolated installation starting at epoch 1, the explicit transition is:

```sql
SELECT transition_execution_writer('legacy', 1, 'paused', 'legacy process stopped; writes drained');
SELECT transition_execution_writer('paused', 2, 'archive', 'verified recording and replay boundary');
```

Read the actual owner/epoch before any later transition. Rollback first stops
admission and the worker, transitions `archive -> paused`, verifies the exact
committed cursor and both-leg economic equality, and restores a compatible OMS
recovery state. Only then may `paused -> legacy` re-enable writes. Retain all
repaired executions, journal rows and checkpoints; do not rewind financial
state. The transition function records every successful ownership change.

The current legacy OMS startup deliberately refuses `paused` or `archive`
ownership. Its durable outcome consumer must be completed before enabling OMS
admission after a production cutover. Current Archive repair tests perform the
ownership transitions only in an isolated schema.

Production writer cutover still requires durable OMS outcome/risk recovery,
history freshness exposure, recording rotation/retention integration, and the
full continuous-load/failover acceptance. A successful history backfill does
not repair live order state or authorize a financial release.
