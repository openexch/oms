# Durable OMS order workflows

Apply `oms-persistence/src/main/resources/db/migration/V006__order_recovery_state.sql`
with the OMS stopped before starting this version. The migration adds workflow
fields and a compare-and-swap `state_revision`; it does not reconstruct missing
history. Existing open rows have revision zero and deliberately block startup.
Do not bulk set that revision to bypass recovery. Reconstruct their state from
verified admission, ME journal and AE hold evidence before allowing admission.

Order saves include trailing extremes, iceberg hidden/current-slice quantities,
cancel intent, hold/parent identity, and all pending replace fields. A stale
snapshot cannot overwrite a newer revision. A failed or uncertain write closes
readiness until recovery. The old partial `updateOrderStatus` write path has been
removed so callers cannot bypass the complete workflow snapshot.

Trigger and iceberg slice intent is committed before enqueue. A triggered stop
leaves `PENDING_TRIGGER` durably and cannot re-arm at startup. Trailing extreme
advances are checkpointed; a missing quote is not a trigger. Cancel and amend
intent precede external commands. Requested amend hold and confirmed amend hold
are separate fields, so a crash or lost acknowledgement cannot turn an attempted
reservation into a known rollback amount. API workflows and lifecycle mutations
use the same per-order monitor.

A confirmed AE hold reject or a failed enqueue returns a rejection. Once enqueued,
timeout, interruption and disconnect retain an unknown outcome. They never send
an automatic full-residual release. A new process cannot classify a hold as a
create versus amend from an in-memory set of acknowledgements.

This is a recovery prerequisite, not a complete rollout contract. Live order/risk
recovery still needs its durable journal barrier, uncertain command resolution,
and settlement/release ownership. Execution projection must not be activated
concurrently with the legacy execution writer. Per-order durable writes are still
synchronous; latency and duty-cycle requirements remain acceptance gates. A
rolling downgrade to an older writer is not supported: it ignores revisions and
would omit workflow fields.

## Durable ME command lane (`OMS_DURABLE_ME_COMMANDS=true`)

Plain LIMIT, MARKET and LIMIT_MAKER creates are sent as `DurableOrderCommand`
(order schema 1/v11). The command identity is derived from the order id and the
workflow revision at `PENDING_NEW`, so it is stable across a crash. The exact
payload is stored in `oms_me_commands` and made `READY` before it is offered;
the matching engine returns the original result for a repeated identity instead
of creating a second order.

- A full submission queue means the command was never offered. It becomes
  `ABORTED` before the order is rejected and its hold released; nothing resends it.
- At startup, open commands are recovered before the cluster session exists.
  Their orders keep admission closed. Every connect and leader change resends
  `READY` commands, because an offered command may not have reached the log.
- Only the canonical outcome that the execution projector writes to
  `me_command_outcomes` resolves a command. An applied outcome links the cluster
  order id; admission reopens only for a resting leg, since fills come from the
  execution stream. An engine rejection or an untouched book rejects the order.

Stops, trailing stops, iceberg slices, cancels and amends still use the legacy
commands. The engine's command ledger admits 100,000 identities without
eviction; enabling this lane beyond that bound needs a replicated retention
protocol. An engine without a configured command journal halts on the first
durable command, so the flag is enabled only after every replica runs v11 with
its journal.

