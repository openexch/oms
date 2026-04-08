package com.openexchange.oms.ledger;

import com.openexchange.oms.common.enums.LedgerEntryType;

/**
 * Immutable ledger entry for the double-entry accounting log.
 * <p>
 * Every balance mutation produces a pair of entries sharing the same {@code journalId}:
 * one debit and one credit. This guarantees the ledger always balances.
 * <p>
 * All monetary amounts use fixed-point representation with 8 decimal places.
 */
public final class LedgerEntry {

    private final long entryId;
    private final long journalId;
    private final long userId;
    private final int assetId;
    private final long amount;
    private final LedgerEntryType entryType;
    private final boolean debit;
    private final long referenceId;
    private final long entryTimeMs;

    public LedgerEntry(long entryId, long journalId, long userId, int assetId, long amount,
                       LedgerEntryType entryType, boolean debit, long referenceId, long entryTimeMs) {
        this.entryId = entryId;
        this.journalId = journalId;
        this.userId = userId;
        this.assetId = assetId;
        this.amount = amount;
        this.entryType = entryType;
        this.debit = debit;
        this.referenceId = referenceId;
        this.entryTimeMs = entryTimeMs;
    }

    public long getEntryId() {
        return entryId;
    }

    public long getJournalId() {
        return journalId;
    }

    public long getUserId() {
        return userId;
    }

    public int getAssetId() {
        return assetId;
    }

    public long getAmount() {
        return amount;
    }

    public LedgerEntryType getEntryType() {
        return entryType;
    }

    public boolean isDebit() {
        return debit;
    }

    public long getReferenceId() {
        return referenceId;
    }

    public long getEntryTimeMs() {
        return entryTimeMs;
    }

    @Override
    public String toString() {
        return "LedgerEntry{" +
                "entryId=" + entryId +
                ", journalId=" + journalId +
                ", userId=" + userId +
                ", assetId=" + assetId +
                ", amount=" + amount +
                ", entryType=" + entryType +
                ", debit=" + debit +
                ", referenceId=" + referenceId +
                ", entryTimeMs=" + entryTimeMs +
                '}';
    }
}
