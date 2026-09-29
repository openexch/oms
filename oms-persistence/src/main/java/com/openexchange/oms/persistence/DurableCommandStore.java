// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

import com.match.domain.commands.DurableOrderIntent;
import java.util.List;

/**
 * Durable ME command outbox as seen by the OMS dispatcher. Every method performs IO;
 * callers run outside the Aeron polling thread.
 */
public interface DurableCommandStore {
    /** A canonical ME result projected from the Archive for an exact stored payload. */
    record Outcome(DurableOrderIntent intent, long orderId, int status, int reason, int result) {}

    /** Store the exact payload as PREPARED; idempotent for the same identity and payload. */
    PostgresCommandRepository.Entry prepare(DurableOrderIntent intent, long revision);

    /** PREPARED becomes sendable. Call only once the hold/workflow precondition is durable. */
    void ready(DurableOrderIntent intent);

    /**
     * Retire a command this process never offered. After this returns, no dispatcher sends it,
     * so its hold may be released. Fails if the command was already resolved.
     */
    void abortUnsent(DurableOrderIntent intent);

    /** PREPARED or READY commands ordered by identity, strictly after the given identity. */
    List<PostgresCommandRepository.Entry> openCommands(long afterHigh, long afterLow, int limit);

    /** READY commands whose exact payload has a projected canonical ME outcome. */
    List<Outcome> projectedOutcomes(int limit);

    /** READY becomes RESOLVED; only after the outcome has been applied to OMS state. */
    void resolve(DurableOrderIntent intent);
}
