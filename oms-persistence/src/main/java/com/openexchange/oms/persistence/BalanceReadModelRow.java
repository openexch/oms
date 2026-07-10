// SPDX-License-Identifier: Apache-2.0
package com.openexchange.oms.persistence;

/**
 * One absolute balance for a {@code (user, asset)} destined for the CQRS balance read model
 * (the {@code account_balances} mirror). All amounts are 8dp fixed-point {@code int64}, matching
 * the AE money schema.
 *
 * <p>This is a projection row for ops/analytics only — it is never money-authoritative and is never
 * read by the money path.</p>
 */
public record BalanceReadModelRow(long userId, int assetId, long available, long locked) {
}
