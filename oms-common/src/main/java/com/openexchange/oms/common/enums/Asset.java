package com.openexchange.oms.common.enums;

/**
 * Supported assets with integer IDs for array indexing.
 */
public enum Asset {
    USD(0),
    BTC(1),
    ETH(2),
    SOL(3),
    XRP(4),
    DOGE(5);

    private final int id;

    Asset(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    private static final Asset[] BY_ID = new Asset[6];
    static {
        for (Asset a : values()) {
            BY_ID[a.id] = a;
        }
    }

    public static Asset fromId(int id) {
        if (id < 0 || id >= BY_ID.length) {
            throw new IllegalArgumentException("Unknown asset ID: " + id);
        }
        return BY_ID[id];
    }

    public static int count() {
        return BY_ID.length;
    }
}
