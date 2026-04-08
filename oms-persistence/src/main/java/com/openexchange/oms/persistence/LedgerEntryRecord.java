package com.openexchange.oms.persistence;

/**
 * Lightweight ledger entry record for persistence operations.
 * Decoupled from the oms-ledger module to avoid circular dependencies.
 */
public record LedgerEntryRecord(
        long entryId,
        long journalId,
        long userId,
        int assetId,
        long amount,
        String entryType,
        boolean isDebit,
        long referenceId,
        long entryTimeMs
) {
}
