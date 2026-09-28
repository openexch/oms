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
