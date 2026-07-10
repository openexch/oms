// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

import java.util.List;

/**
 * Write seam for the CQRS balance read model: a durable, queryable mirror of AE balances used by
 * ops/analytics. It is fed a coalesced batch of last-write-wins absolutes and upserts them.
 *
 * <p>Isolating the upsert behind this interface lets the off-thread writer be unit-tested against a
 * fake/mock sink (the repo has no PG integration-test harness — no testcontainers/embedded-pg), and
 * keeps all JDBC in {@code oms-persistence}.</p>
 *
 * <p><b>Not money-authoritative.</b> This mirror is never read by the money path; the AE (and the
 * OMS {@code BalanceProjection} it feeds) remain the record of balances.</p>
 */
public interface BalanceReadModelStore {

    /**
     * Upsert a batch of absolute balances into the read model in one transaction. Each row replaces
     * the stored {@code (available, locked)} for its {@code (user, asset)} key.
     *
     * @param rows the batch (may be empty — implementations must treat empty as a no-op)
     * @throws Exception if the write fails; the caller keeps the rows dirty and retries on the next tick
     */
    void upsert(List<BalanceReadModelRow> rows) throws Exception;
}
