# Durable admission and recovery guards

Apply `oms-persistence/src/main/resources/db/migration/V005__durable_order_requests.sql`
with `psql -v ON_ERROR_STOP=1` before enabling API requests with `requestId`.
PostgreSQL and successful risk/order/position recovery are required at startup.

`requestId` is an optional REST field and gRPC field 13. It is scoped to the
authenticated user and must be retained when retrying the same command. Its
completed response survives terminal orders and process restarts. A different
payload under the same key is rejected. An incomplete durable claim returns
an unavailable response and never blindly repeats the hold or ME submission.
It needs explicit outcome recovery; there is no timeout-based claim takeover.

`clientOrderId` retains its existing active-order-label semantics and can be
reused after terminalization. It does not provide durable retry identity.
Callers that need retry safety must send `requestId`.

Creation commits the initial order intent before a hold, commits the hold
stage before attempting that external operation, and saves an accepted
stop-order intent before arming it. A write failure closes admission for that
process. A persisted pre-hold intent is unresolved on restart: startup stops
instead of dropping the row or releasing its possible hold. Complete automatic
recovery of trigger, refill, amend and cancel workflows remains separate work.

An open-order snapshot can positively relink an order. Its absence cannot
prove cancellation. Replace timeouts preserve their unresolved hold, and
GTD expiry of a submitted order requests cancellation and waits for an outcome.
The hold scanner reports discrepancies without releasing money from absence
or age. These guards can deliberately leave orders and funds unresolved until
authoritative recovery is available.

`/ready` and `/api/v1/ready` return 503 if the admission guard, ME connection,
or AE boot projection is unavailable. Database evidence is refreshed on a
separate thread and expires after three seconds; API probes do not perform
database I/O. A failed durable order write remains latched even after a DB
probe succeeds. This local guard does not establish completeness of the
legacy execution history: the Archive outcome barrier and writer handoff
must be integrated before qualifying the new architecture for rollout.
