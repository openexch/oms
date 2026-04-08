package com.openexchange.oms.common.domain;

import com.openexchange.oms.common.enums.Asset;

/**
 * Market definition mapping marketId to base/quote asset pair.
 */
public record Market(int marketId, String symbol, Asset baseAsset, Asset quoteAsset) {

    public static final Market BTC_USD = new Market(1, "BTC-USD", Asset.BTC, Asset.USD);
    public static final Market ETH_USD = new Market(2, "ETH-USD", Asset.ETH, Asset.USD);
    public static final Market SOL_USD = new Market(3, "SOL-USD", Asset.SOL, Asset.USD);
    public static final Market XRP_USD = new Market(4, "XRP-USD", Asset.XRP, Asset.USD);
    public static final Market DOGE_USD = new Market(5, "DOGE-USD", Asset.DOGE, Asset.USD);

    public static final Market[] ALL = {BTC_USD, ETH_USD, SOL_USD, XRP_USD, DOGE_USD};

    private static final Market[] BY_ID = new Market[6];
    static {
        for (Market m : ALL) {
            BY_ID[m.marketId] = m;
        }
    }

    public static Market fromId(int marketId) {
        if (marketId < 1 || marketId >= BY_ID.length || BY_ID[marketId] == null) {
            throw new IllegalArgumentException("Unknown market ID: " + marketId);
        }
        return BY_ID[marketId];
    }
}
