package com.openexchange.oms.common.enums;

/**
 * Types of ledger entries for double-entry accounting.
 */
public enum LedgerEntryType {
    DEPOSIT,
    WITHDRAWAL,
    ORDER_HOLD,
    ORDER_RELEASE,
    TRADE_DEBIT,
    TRADE_CREDIT,
    FEE
}
