package com.openexchange.oms.common.enums;

/**
 * Time-in-force instructions for orders.
 */
public enum TimeInForce {
    /** Good-Till-Cancelled: stays active until filled or explicitly cancelled */
    GTC,
    /** Good-Till-Date: stays active until filled, cancelled, or expiry time */
    GTD,
    /** Immediate-Or-Cancel: fill what's available immediately, cancel rest */
    IOC,
    /** Fill-Or-Kill: must fill entirely immediately or cancel completely */
    FOK
}
